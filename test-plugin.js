const { io } = require("socket.io-client");

const socket = io(
    "http://127.0.0.1:3000/plugin-websocket",
    {
        path: "/plugin-websocket/socket.io",

        transports: ["polling"],

        upgrade: false,

        reconnection: false,

        timeout: 10000,

        query: {
            pluginId: "TEST-PLUGIN-001",
            pluginName: "AnarSystem Test Plugin",
            departmentId: "TEST-DEPARTMENT-001",
            departmentName: "Test Department",
            groupId: "TEST-GROUP-001",
            groupName: "Test Group",
            version: "TEST-1.0.0",
            currencyCode: "AZN"
        }
    }
);


// ============================================================
// CONNECT
// ============================================================

socket.on("connect", () => {

    console.log("");
    console.log("=================================");
    console.log("TEST PLUGIN CONNECTED");
    console.log("=================================");

    console.log("Socket ID:", socket.id);

    console.log("Namespace:", socket.nsp);

    console.log("Connected:", socket.connected);

    console.log("=================================");


    // --------------------------------------------------------
    // Отправляем identity
    // --------------------------------------------------------

    socket.emit(
        "plugin_to_server",
        {
            pluginId: "TEST-PLUGIN-001",
            pluginName: "AnarSystem Test Plugin",

            departmentId:
                "TEST-DEPARTMENT-001",

            departmentName:
                "Test Department",

            groupId:
                "TEST-GROUP-001",

            groupName:
                "Test Group",

            version:
                "TEST-1.0.0",

            currencyCode:
                "AZN",

            serverUrl:
                "http://test-iiko-server"
        }
    );


    // --------------------------------------------------------
    // Тестовое событие
    // --------------------------------------------------------

    socket.emit(
        "plugin_to_server_event",
        {
            pluginEventType:
                "test_event",

            uuid:
                "test-event-001",

            message:
                "Hello from test plugin",

            time:
                new Date().toISOString()
        }
    );


    console.log("");
    console.log(
        "Test identity and event sent."
    );


    // --------------------------------------------------------
    // Ждём команды от VPS
    // --------------------------------------------------------

    socket.on(
        "server_to_plugin_request",
        (request) => {

            console.log("");
            console.log(
                "========== SERVER REQUEST =========="
            );

            console.log(
                JSON.stringify(
                    request,
                    null,
                    2
                )
            );


            let data;


            switch (request.action) {

                case "get_sales":

                    data = {

                        dateFrom:
                            request.params?.dateFrom ||
                            null,

                        dateTo:
                            request.params?.dateTo ||
                            null,

                        total:
                            1500,

                        currency:
                            "AZN",

                        source:
                            "TEST PLUGIN"
                    };

                    break;


                case "get_orders":

                    data = {

                        orders: [

                            {
                                id:
                                    "TEST-ORDER-001",

                                number:
                                    "1001",

                                total:
                                    125.50,

                                status:
                                    "Closed"
                            },

                            {
                                id:
                                    "TEST-ORDER-002",

                                number:
                                    "1002",

                                total:
                                    75.00,

                                status:
                                    "Closed"
                            }

                        ],

                        source:
                            "TEST PLUGIN"
                    };

                    break;


                case "get_payments":

                    data = {

                        payments: [

                            {
                                type:
                                    "Cash",

                                amount:
                                    100
                            },

                            {
                                type:
                                    "Card",

                                amount:
                                    100.50
                            }

                        ],

                        source:
                            "TEST PLUGIN"
                    };

                    break;


                case "get_products":

                    data = {

                        products: [

                            {
                                id:
                                    "PRODUCT-001",

                                name:
                                    "Test Product",

                                price:
                                    10
                            },

                            {
                                id:
                                    "PRODUCT-002",

                                name:
                                    "Test Product 2",

                                price:
                                    25
                            }

                        ],

                        source:
                            "TEST PLUGIN"
                    };

                    break;


                case "get_employees":

                    data = {

                        employees: [

                            {
                                id:
                                    "EMP-001",

                                name:
                                    "Test Employee"
                            }

                        ],

                        source:
                            "TEST PLUGIN"
                    };

                    break;


                default:

                    data = {

                        message:
                            "Unknown action",

                        action:
                            request.action,

                        source:
                            "TEST PLUGIN"
                    };

                    break;
            }


            // ------------------------------------------------
            // Отправляем response обратно VPS
            // ------------------------------------------------

            socket.emit(
                "plugin_to_server_response",
                {
                    requestId:
                        request.requestId,

                    success:
                        true,

                    data:
                        data
                }
            );


            console.log("");

            console.log(
                "Response sent:"
            );

            console.log(
                "Request ID:",
                request.requestId
            );

        }
    );
});


// ============================================================
// ERROR
// ============================================================

socket.on(
    "connect_error",
    (error) => {

        console.log("");

        console.log(
            "================================="
        );

        console.log(
            "TEST PLUGIN CONNECTION ERROR"
        );

        console.log(
            "================================="
        );

        console.log(
            "Message:",
            error.message
        );

        console.log(
            "Description:",
            error.description
        );

        console.log(
            "================================="
        );
    }
);


// ============================================================
// DISCONNECT
// ============================================================

socket.on(
    "disconnect",
    (reason) => {

        console.log("");

        console.log(
            "TEST PLUGIN DISCONNECTED"
        );

        console.log(
            "Reason:",
            reason
        );

        process.exit(0);
    }
);
