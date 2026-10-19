#!/usr/bin/env bash
# Safe continuation after the Cloudflare token was rejected by the first installer.
# POS plugin binaries and legacy Socket.IO connections remain unchanged.
set -Eeuo pipefail
umask 077
cd /home/ubuntu/anarsystem/backend
ENVFILE=/etc/anarsystem-api.env
SERVICE=anarsystem-api.service
WEBHOOK=https://smarthoreca.pages.dev/api/plugin/ingest
die(){ echo "ОСТАНОВЛЕНО: $*" >&2; exit 1; }

[[ "$(git rev-parse HEAD)" == "19ea5641b2fceba3cce7d8cc55ef6d748233c36c" ]] || die "Версия VPS изменилась, требуется отдельная проверка."
[[ "$(git branch --show-current)" == "feature/socket-auth-auto-enroll-v1" ]] || die "Неожиданная рабочая ветка."
[[ -f server-core.js && -f owner-event-forwarder.js ]] || die "Первый установщик не подготовил код."
sudo test -f "$ENVFILE" || die "Файл среды VPS не найден."
sudo systemctl is-active --quiet "$SERVICE" || die "VPS служба уже остановлена; не трогаю её."

TEMP="$(mktemp -d)"
trap 'rm -rf "$TEMP"' EXIT
git show FETCH_HEAD:server-core.js > "$TEMP/expected-core.js"
git show FETCH_HEAD:owner-event-forwarder.js > "$TEMP/expected-forwarder.js"
cmp -s server-core.js "$TEMP/expected-core.js" || die "Код VPS не совпадает с проверенным PR."
cmp -s owner-event-forwarder.js "$TEMP/expected-forwarder.js" || die "Модуль VPS не совпадает с проверенным PR."
node --check server-core.js
node --check owner-event-forwarder.js
git show HEAD:server-core.js > "$TEMP/original-core.js"

REGISTRY="$(curl -fsS --max-time 8 http://127.0.0.1:3000/api/plugin/data)" || die "Рабочий API VPS не отвечает."
DEPARTMENT_ID="$(printf '%s' "$REGISTRY" | python3 -c '
import json,re,sys
rows=json.load(sys.stdin).get("plugins",[])
p=[x for x in rows if x.get("pluginId")=="b557baad-f85c-4f40-a247-12f8855951ee" and x.get("online") is True]
assert len(p)==1,"Касса TESTING KASSA не онлайн"
d=str(p[0].get("departmentId",""))
assert re.fullmatch("[a-fA-F0-9-]{36}",d),"Ошибка departmentId"
print(d)
')" || die "Тестовая касса недоступна."

BACKUP="$(sudo mktemp -d /var/backups/sh-rms2-push-resume.XXXXXXXX)"
sudo chmod 700 "$BACKUP"
sudo cp -a "$ENVFILE" "$BACKUP/env-before-rotation"
sudo cp -a server-core.js "$BACKUP/staged-server-core.js"
TOKEN="$(python3 -c 'import secrets;print(secrets.token_hex(32))')"

cat > "$TEMP/writeenv.py" <<'PY'
import os,re,stat,sys,tempfile
path=sys.argv[1]; token=sys.stdin.read().strip()
assert re.fullmatch("[a-f0-9]{64}",token)
st=os.stat(path)
with open(path,encoding="utf-8") as f: lines=f.readlines()
names=("SH_PUSH_EVENT_WEBHOOK_URL","SH_PUSH_EVENT_WEBHOOK_TOKEN","SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS")
pat=re.compile(r"^\s*(?:export\s+)?(?:"+"|".join(names)+r")\s*=")
lines=[s for s in lines if not pat.match(s)]
entries=[
"SH_PUSH_EVENT_WEBHOOK_URL=https://smarthoreca.pages.dev/api/plugin/ingest",
"SH_PUSH_EVENT_WEBHOOK_TOKEN="+token,
"SH_PUSH_EVENT_ALLOWED_PLUGIN_IDS=b557baad-f85c-4f40-a247-12f8855951ee"
]
fd,tmp=tempfile.mkstemp(prefix=".sh-push-env-",dir=os.path.dirname(path))
try:
    os.fchmod(fd,stat.S_IMODE(st.st_mode))
    os.fchown(fd,st.st_uid,st.st_gid)
    with os.fdopen(fd,"w",encoding="utf-8") as f:
        f.writelines(lines)
        if lines and not lines[-1].endswith("\n"):f.write("\n")
        f.write("\n".join(entries)+"\n")
    os.replace(tmp,path)
except:
    if os.path.exists(tmp):os.unlink(tmp)
    raise
PY
printf %s "$TOKEN" | sudo python3 "$TEMP/writeenv.py" "$ENVFILE" || die "Невозможно обновить токен VPS."

echo
echo "НОВЫЙ СЕКРЕТ (СКОПИРУЙ В CLOUDFLARE, НО НЕ ПРИСЫЛАЙ В ЧАТ):"
echo "$TOKEN"
echo
echo "Cloudflare → smarthoreca → Settings → Variables and secrets → Production"
echo "ЗАМЕНИ значение PLUGIN_INGEST_TOKEN этим новым ключом, затем Save."
echo "Если Cloudflare пишет 'next deployment', Redeploy последнего Production."
echo "VPS пока продолжает работать, перезапуск не выполнялся."
read -r -p "Когда Cloudflare обновится, нажми ENTER здесь: " _

# Intentionally invalid plugin ID. Correct token + valid registry yields HTTP
# 403 PLUGIN_DEPARTMENT_MISMATCH without event persistence or FCM.
PROBE="$(printf '{"event":"pilotTokenCheck","departmentId":"%s","pluginId":"00000000-0000-4000-8000-000000000000","data":{"probe":true}}' "$DEPARTMENT_ID")"
ATTEMPT=0
while :; do
  ATTEMPT=$((ATTEMPT+1))
  CODE="$(curl -sS --max-time 12 -o "$TEMP/probe.json" -w '%{http_code}' -X POST "$WEBHOOK" -H 'Content-Type: application/json' -H "X-Plugin-Token: $TOKEN" --data-binary "$PROBE" 2>/dev/null || true)"
  RESULT="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("code",""))' "$TEMP/probe.json" 2>/dev/null || true)"
  if [[ "$CODE" == 403 && "$RESULT" == PLUGIN_DEPARTMENT_MISMATCH ]]; then
    echo "Cloudflare подтвердил секрет. Тестовое событие не сохранено."
    break
  fi
  if ((ATTEMPT % 5 == 0)); then
    echo "Cloudflare ещё не принял секрет (HTTP $CODE, код $RESULT). VPS НЕ перезапущен."
    read -r -p "Проверь Cloudflare Save/Redeploy и нажми ENTER для новой проверки (Ctrl+C — выход): " _
  else
    sleep 4
  fi
done

rollback(){
  echo "Возвращаю исходный код VPS."
  cp "$TEMP/original-core.js" server-core.js
  sudo cp -a "$BACKUP/env-before-rotation" "$ENVFILE"
  rm -f owner-event-forwarder.js
  sudo systemctl restart "$SERVICE" || true
  echo "Резервная копия: $BACKUP"
}
echo "Перезапускаю только VPS API, кассовые плагины не меняю."
if ! sudo systemctl restart "$SERVICE"; then
  rollback
  die "Перезапуск VPS не удался."
fi
READY=0
for i in $(seq 1 18); do
  if sudo systemctl is-active --quiet "$SERVICE" &&
     curl -fsS --max-time 4 http://127.0.0.1:3000/api/plugin/data >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 4
done
if [[ "$READY" -ne 1 ]]; then
  rollback
  die "Новый VPS API не отвечает."
fi
echo "VPS API работает. Ожидаю TESTING KASSA онлайн..."
ONLINE=0
for i in $(seq 1 18); do
  if curl -fsS --max-time 4 http://127.0.0.1:3000/api/plugin/data |
    python3 -c '
import json,sys
try:
    p=json.load(sys.stdin).get("plugins",[])
    sys.exit(0 if any(x.get("pluginId")=="b557baad-f85c-4f40-a247-12f8855951ee" and x.get("online") is True for x in p) else 1)
except Exception:sys.exit(1)
' 2>/dev/null; then
      ONLINE=1
      break
  fi
  sleep 5
done
if [[ "$ONLINE" -ne 1 ]]; then
  rollback
  die "TESTING KASSA не переподключилась, откат выполнен."
fi
echo "УСПЕХ: VPS и TESTING KASSA онлайн; пуш-события только от этой кассы."
echo "Резервная копия: $BACKUP"
echo "Теперь открой Android Smart Horeca Owner → Уведомления → Применена скидка."
