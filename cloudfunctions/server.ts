import { createApiServer } from "./src/app.ts";

const PORT = Number(process.env.PORT || 9040);
const HOST = process.env.HOST || "0.0.0.0";

const server = createApiServer();

server.listen(PORT, HOST, () => {
  console.log(`[usvm-api] listening on http://${HOST}:${PORT}`);
});
