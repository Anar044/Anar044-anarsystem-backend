#!/usr/bin/env bash
# One-paste installer: preserves all legacy POS connections.
# Replace Cloudflare secret in browser before agreeing to restart.
set -Eeuo pipefail
umask 077
cd /home/ubuntu/anarsystem/backend
die(){ echo "ОСТАНОВЛЕНО: $*" >&2; exit 1; }
[[ "$(git rev-parse HEAD)" == "19ea5641b2fceba3cce7d8cc55ef6d748233c36c" ]] || die "Версия VPS изменилась. Никаких изменений."
[[ "$(git branch --show-current)" == "feature/socket-auth-auto-enroll-v1" ]] || die "Неожиданная ветка VPS."
git diff --quiet HEAD -- server-core.js || die "Есть местные изменения server-core.js."
git diff --cached --quiet HEAD -- server-core.js || die "Есть несохранённые изменения server-core.js."
[[ ! -e owner-event-forwarder.js ]] || die "Модуль уже существует, не перезаписываю."
sudo test -f /etc/anarsystem-api.env || die "Нет конфигурации VPS."
command -v node >/dev/null && command -v python3 >/dev/null && command -v curl >/dev/null || die "Необходимые программы недоступны."
REGISTRY="$(curl -fsS --max-time 10 http://127.0.0.1:3000/api/plugin/data)" || die "VPS API не отвечает."
DEPARTMENT_ID="$(printf '%s' "$REGISTRY" | python3 -c '
import json,sys,re
rows=json.load(sys.stdin).get("plugins",[])
matches=[p for p in rows if p.get("pluginId")=="b557baad-f85c-4f40-a247-12f8855951ee" and p.get("online") is True]
assert len(matches)==1,"TESTING KASSA должна быть онлайн"
did=str(matches[0].get("departmentId",""))
assert re.fullmatch(r"[0-9a-fA-F-]{36}",did)
print(did)
')" || die "Касса TESTING KASSA не найдена онлайн."

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
git show FETCH_HEAD:server-core.js > "$TMP/server-core.js"
git show FETCH_HEAD:owner-event-forwarder.js > "$TMP/owner-event-forwarder.js"
node --check "$TMP/server-core.js" && node --check "$TMP/owner-event-forwarder.js" || die "Исходники не прошли проверку."
BACKUP="$(sudo mktemp -d /var/backups/sh-rms2-push.XXXXXXXX)"
sudo chmod 700 "$BACKUP"
sudo cp -a /etc/anarsystem-api.env "$BACKUP/env"
sudo cp -a server-core.js "$BACKUP/server-core.js"
TOKEN="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
[[ "$(printf '%s' "$TOKEN" | wc -c)" -eq 64 ]] || die "Ошибка генерации ключа"

cat > "$TMP/save_env.py" <<'PY'
import os,re,sys,tempfile
path=sys.argv[1]
token=sys.stdin.read().strip()
assert re.fullmatch("[0-9a-f]{64}",token)
with open(path,encoding="utf-8") as f: lines=f.readlines()
keys="SH_PUSH_EVENT_WEBHOOK_URL|SH_PUSH_EVENT_WEBHOOK_TOKEN|SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS"
lines=[s for s in lines if not re.match(r"^\s*(?:export\s+)?("+keys+r")\s*=",s)]
values=[
"SH_PUSH_EVENT_WEBHOOK_URL=https://smarthoreca.pages.dev/api/plugin/ingest",
"SH_PUSH_EVENT_WEBHOOK_TOKEN="+token,
"SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS=b557baad-f85c-4f40-a247-12f8855951ee"
]
fd,tmp=tempfile.mkstemp(prefix=".sh-env-",dir=os.path.dirname(path))
try:
    os.fchmod(fd,0o600)
    with os.fdopen(fd,"w",encoding="utf-8") as out:
        out.writelines(lines)
        if lines and not lines[-1].endswith("\n"):out.write("\n")
        out.write("\n".join(values)+"\n")
    os.replace(tmp,path)
except:
    if os.path.exists(tmp):os.remove(tmp)
    raise
PY

cp "$TMP/server-core.js" server-core.js
install -m 644 "$TMP/owner-event-forwarder.js" owner-event-forwarder.js
printf '%s' "$TOKEN" | sudo python3 "$TMP/save_env.py" /etc/anarsystem-api.env || die "Не смог сохранить новый ключ."
echo
echo "============================================================"
echo "НОВЫЙ КЛЮЧ — СКОПИРУЙ В CLOUDFLARE, НЕ ПРИСЫЛАЙ В ЧАТ:"
echo "$TOKEN"
echo "============================================================"
echo "Cloudflare → smarthoreca → Production → Variables and secrets:"
echo "ЗАМЕНИ Secret PLUGIN_INGEST_TOKEN этим ключом и сохрани."
echo "Если написано 'next deployment', сделай Redeploy последнего Production."
echo "Рабочий VPS пока НЕ перезапускался."
echo
read -r -p "Когда Cloudflare будет обновлён, нажми здесь ENTER: " _

echo "Проверяю Cloudflare без записи кассовых событий..."
PROBE="$(printf '{"event":"pilotTokenCheck","departmentId":"%s","pluginId":"00000000-0000-4000-8000-000000000000","data":{"probe":true}}' "$DEPARTMENT_ID")"
VALID=0
for i in $(seq 1 30); do
  CODE="$(curl -sS --max-time 12 -o "$TMP/probe.json" -w '%{http_code}' -X POST \
    https://smarthoreca.pages.dev/api/plugin/ingest \
    -H 'Content-Type: application/json' -H "X-Plugin-Token: $TOKEN" \
    --data-binary "$PROBE" 2>/dev/null || true)"
  RESULT="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("code",""))' "$TMP/probe.json" 2>/dev/null || true)"
  if [[ "$CODE" == "403" && "$RESULT" == "PLUGIN_DEPARTMENT_MISMATCH" ]]; then
    VALID=1
    break
  fi
  if ((i % 6 == 0)); then
    echo "Жду публикацию нового Cloudflare Secret. Проверка $i из 30, HTTP $CODE, ответ $RESULT"
  fi
  sleep 10
done
if [[ "$VALID" != "1" ]]; then
  echo "Cloudflare ещё не подтвердил новый ключ. VPS НЕ перезапущен." >&2
  echo "Резервная копия: $BACKUP" >&2
  exit 2
fi

echo "Ключ подтверждён. Перезапускаю только VPS API..."
if ! sudo systemctl restart anarsystem-api.service; then
  echo "Перезапуск не удался, восстанавливаю предыдущую версию" >&2
  sudo cp -a "$BACKUP/server-core.js" server-core.js
  sudo cp -a "$BACKUP/env" /etc/anarsystem-api.env
  rm -f owner-event-forwarder.js
  sudo systemctl restart anarsystem-api.service || true
  die "Откат выполнен"
fi
UP=0
for i in $(seq 1 18); do
  if sudo systemctl is-active --quiet anarsystem-api.service &&
    curl -fsS --max-time 3 http://127.0.0.1:3000/api/plugin/data >/dev/null 2>&1; then UP=1;break;fi
  sleep 4
done
if [[ "$UP" != 1 ]]; then
  echo "API не ответил после обновления, выполняю откат" >&2
  sudo cp -a "$BACKUP/server-core.js" server-core.js
  sudo cp -a "$BACKUP/env" /etc/anarsystem-api.env
  rm -f owner-event-forwarder.js
  sudo systemctl restart anarsystem-api.service || true
  die "Откат выполнен"
fi
echo "VPS API работает. Ожидаю переподключение тестовой кассы..."
ONLINE=0
for i in $(seq 1 20); do
  if curl -fsS --max-time 4 http://127.0.0.1:3000/api/plugin/data | python3 -c '
import json,sys
try:
    rows=json.load(sys.stdin).get("plugins",[])
    ok=any(p.get("pluginId")=="b557baad-f85c-4f40-a247-12f8855951ee" and p.get("online") is True for p in rows)
    sys.exit(0 if ok else 1)
except Exception:sys.exit(1)
' 2>/dev/null; then ONLINE=1;break;fi
  sleep 5
done
if [[ "$ONLINE" == 1 ]]; then
  echo "ГОТОВО: VPS и TESTING KASSA онлайн. Только её события отправляются в push."
else
  echo "VPS работает, но TESTING KASSA пока переподключается. Перед тестом проверь её онлайн."
fi
echo "Резервная копия: $BACKUP"
echo "Теперь на Android включи 'Применена скидка' в разделе Уведомления."
