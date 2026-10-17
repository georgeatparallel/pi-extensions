/** Run after installing pi's packages: see README.md. No live requests. */
import assert from "node:assert/strict";
import { test } from "node:test";
import { readFile, rm } from "node:fs/promises";
import { Value } from "typebox/value";
import extension from "../parallel-search.ts";

const ENDPOINT = "https://search.parallel.ai/mcp";
const USER_AGENT = "pi-extensions/parallel-search (https://github.com/hexx/pi-extensions)";
const SESSION = "99b6ed75-4394-45c3-a92b-c6d63c62cb3c";
const ctx = { sessionManager: { getSessionId: () => SESSION }, model: { id: "test-model-exact" } };
function load() {
	const tools = new Map<string, any>();
	extension({ registerTool: (tool: any) => tools.set(tool.name, tool) } as any);
	return tools;
}
const searchArgs = { objective: "Find Node.js LTS documentation", search_queries: ["Node.js LTS official documentation"] };
const fetchArgs = { urls: ["https://nodejs.org/en/about/previous-releases"], objective: "Find the LTS policy" };
function json(id: number, result: unknown, headers = {}) {
	return new Response(JSON.stringify({ jsonrpc: "2.0", id, result }), { headers: { "content-type": "application/json", ...headers } });
}
function mockServer(t: any, handler: (body: any, init: RequestInit) => Response | Promise<Response>) {
	const requests: any[] = [];
	t.mock.method(globalThis, "fetch", async (url: string, init: RequestInit) => {
		assert.equal(url, ENDPOINT);
		const headers = new Headers(init.headers);
		assert.equal(headers.get("User-Agent"), USER_AGENT);
		assert.equal(headers.get("Authorization"), null);
		assert.equal(headers.get("x-api-key"), null);
		assert.equal(headers.get("Accept"), "application/json, text/event-stream");
		assert.equal(init.method, "POST");
		const body = JSON.parse(init.body as string);
		requests.push({ body, headers });
		return handler(body, init);
	});
	return requests;
}
function handshake(body: any): Response | undefined {
	if (body.method === "initialize") return json(1, { protocolVersion: "2025-03-26" }, { "mcp-session-id": "transport-session" });
	if (body.method === "notifications/initialized") return new Response(null, { status: 202 });
}
function run(tools: Map<string, any>, name: string, args: any, signal?: AbortSignal) {
	return tools.get(name).execute("test-call", args, signal, undefined, ctx);
}

test("registration is opt-in, offline, and uses distinct tools and valid schemas", (t) => {
	t.mock.method(globalThis, "fetch", () => { throw new Error("network at load"); });
	const tools = load();
	assert.deepEqual([...tools.keys()], ["parallel_search", "parallel_fetch"]);
	assert.equal(Value.Check(tools.get("parallel_search").parameters, searchArgs), true);
	assert.equal(Value.Check(tools.get("parallel_search").parameters, { objective: "x", search_queries: [] }), false);
	assert.equal(Value.Check(tools.get("parallel_fetch").parameters, fetchArgs), true);
	assert.equal(Value.Check(tools.get("parallel_fetch").parameters, { urls: ["file:///etc/passwd"] }), false);
	assert.equal(Value.Check(tools.get("parallel_fetch").parameters, { urls: Array(21).fill("https://example.com") }), false);
});

test("registered search and fetch dispatch useful JSON output without credentials, preserving conversation metadata", async (t) => {
	const requests = mockServer(t, (body) => handshake(body) ?? json(2, { content: [{ type: "text", text: "Node.js LTS: https://nodejs.org/en/about/previous-releases" }] }));
	const tools = load();
	for (const [name, args, remote] of [["parallel_search", searchArgs, "web_search"], ["parallel_fetch", fetchArgs, "web_fetch"]] as const) {
		const result = await run(tools, name, args);
		assert.match(result.content[0].text, /Node.js LTS: https:\/\/nodejs.org/);
		const call = requests.at(-1);
		assert.deepEqual(call.body.params, { name: remote, arguments: { ...args, session_id: SESSION, model_name: "test-model-exact" } });
		assert.equal(call.headers.get("Mcp-Session-Id"), "transport-session");
		assert.equal(call.headers.get("MCP-Protocol-Version"), "2025-03-26");
	}
	assert.deepEqual(requests.map((r) => r.body.method), ["initialize", "notifications/initialized", "tools/call", "initialize", "notifications/initialized", "tools/call"]);
	assert.equal(requests[0].headers.get("Mcp-Session-Id"), null);
	assert.equal(requests[1].body.id, undefined);
});

test("SSE handles split chunks, CRLF, multiline data and unrelated messages, then closes an open stream", async (t) => {
	let cancelled = false;
	mockServer(t, (body) => handshake(body) ?? new Response(new ReadableStream({
		start(controller) {
			const raw = ': ping\r\ndata: {"jsonrpc":"2.0","method":"notice"}\r\n\r\ndata: {"jsonrpc":"2.0","id":99,"result":{}}\r\n\r\ndata: {"jsonrpc":"2.0","id":2,\r\ndata: "result":{"content":[{"type":"text","text":"ページの抜粋"}]}}\r\n\r\n';
			const bytes = new TextEncoder().encode(raw);
			for (let i = 0; i < bytes.length; i += 7) controller.enqueue(bytes.slice(i, i + 7));
		}, cancel() { cancelled = true; },
	}), { headers: { "content-type": "text/event-stream" } }));
	assert.equal((await run(load(), "parallel_fetch", fetchArgs)).content[0].text, "ページの抜粋");
	assert.equal(cancelled, true);
});

test("structured results are retained when text content is absent", async (t) => {
	mockServer(t, (body) => handshake(body) ?? json(2, { structuredContent: { results: [{ url: "https://example.com", excerpts: ["useful source"] }] } }));
	assert.match((await run(load(), "parallel_search", searchArgs)).content[0].text, /useful source/);
});

for (const [label, response, message] of [
	["quota", () => new Response("limited", { status: 429, headers: { "retry-after": "60" } }), /HTTP 429.*Retry-After: 60/],
	["HTTP error", () => new Response("unavailable", { status: 503 }), /HTTP 503/],
	["RPC error", () => new Response(JSON.stringify({ jsonrpc: "2.0", id: 2, error: { code: -32602, message: "bad arguments" } })), /-32602: bad arguments/],
	["tool error", () => json(2, { isError: true, content: [{ type: "text", text: "quota exceeded" }] }), /failed: quota exceeded/],
	["wrong id", () => json(99, { content: [] }), /unexpected JSON-RPC/],
	["invalid JSON", () => new Response("not json"), /JSON|Unexpected token/],
	["missing result", () => new Response('{"jsonrpc":"2.0","id":2}'), /no result/],
	["empty content", () => json(2, { content: [] }), /no text/],
	["SSE missing id", () => new Response('data: {"jsonrpc":"2.0","id":99,"result":{}}\n\n', { headers: { "content-type": "text/event-stream" } }), /without a matching response/],
] as const) {
	test(`${label} propagates as a tool failure without retries`, async (t) => {
		const requests = mockServer(t, (body) => handshake(body) ?? response());
		await assert.rejects(run(load(), "parallel_search", searchArgs), message);
		assert.equal(requests.length, 3);
	});
}

test("initialization errors stop before tools/call", async (t) => {
	const requests = mockServer(t, () => json(1, { protocolVersion: "unsupported" }));
	await assert.rejects(run(load(), "parallel_search", searchArgs), /unsupported protocol/);
	assert.equal(requests.length, 1);
});

test("pre-cancelled calls send no requests", async (t) => {
	const requests = mockServer(t, () => { throw new Error("unexpected request"); });
	await assert.rejects(run(load(), "parallel_search", searchArgs, AbortSignal.abort()), { name: "AbortError" });
	assert.equal(requests.length, 0);
});

for (const phase of ["headers", "body"] as const) {
	for (const mode of ["cancel", "timeout"] as const) {
		test(`${mode} interrupts waiting for ${phase}`, async (t) => {
			const controller = new AbortController();
			if (mode === "timeout") t.mock.method(AbortSignal, "timeout", (ms: number) => {
				assert.equal(ms, 60_000);
				return controller.signal;
			});
			mockServer(t, (body, init) => {
				const early = handshake(body);
				if (early) return early;
				const reason = new DOMException(mode, mode === "timeout" ? "TimeoutError" : "AbortError");
				setImmediate(() => controller.abort(reason));
				if (phase === "headers") return new Promise((_resolve, reject) => init.signal!.addEventListener("abort", () => reject(init.signal!.reason), { once: true }));
				return new Response(new ReadableStream({ start(stream) {
					init.signal!.addEventListener("abort", () => stream.error(init.signal!.reason), { once: true });
				} }), { headers: { "content-type": "text/event-stream" } });
			});
			await assert.rejects(run(load(), "parallel_search", searchArgs, mode === "cancel" ? controller.signal : undefined), { name: mode === "timeout" ? "TimeoutError" : "AbortError" });
		});
	}
}

test("large output is bounded and the full UTF-8 result is saved privately", async (t) => {
	const text = "出典 https://example.com\n".repeat(4000);
	mockServer(t, (body) => handshake(body) ?? json(2, { content: [{ type: "text", text }] }));
	const result = await run(load(), "parallel_search", searchArgs);
	const path = result.details.fullOutputPath;
	try {
		assert.match(result.content[0].text, /Output truncated/);
		assert.ok(Buffer.byteLength(result.content[0].text) < 52 * 1024);
		assert.equal(await readFile(path, "utf8"), text.trim());
	} finally { await rm(path.slice(0, path.lastIndexOf("/")), { recursive: true, force: true }); }
});
