import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [vue()],
  // Vite listens on IPv4 only, and a browser can resolve `localhost` to `::1`: the printed link
  // names the address it serves.
  server: { host: "127.0.0.1" },
  // The SDK's source rather than its build, so the example needs no SDK build and follows SDK
  // changes as they are made. An app outside this repo imports the package as usual.
  resolve: {
    alias: { "@hergmes/sdk": new URL("../../src/index.ts", import.meta.url).pathname },
  },
});
