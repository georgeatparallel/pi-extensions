/**
 * Keyless Parallel Search MCP tools for pi. Load this file explicitly with -e,
 * or copy/symlink it into an extensions directory. No network requests at load.
 */
import type { ExtensionAPI, ExtensionToolContext } from "@earendil-works/pi-coding-agent";
import { truncateHead } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const ENDPOINT = "https://search.parallel.ai/mcp";
const USER_AGENT = "pi-extensions/parallel-search (https://github.com/hexx/pi-extensions)";
const PROTOCOL = "2025-03-26";
const TIMEOUT_MS = 60_000;

type RpcMessage = {
	jsonrpc?: string;
	id?: number;
	result?: unknown;
	error?: { code?: number; message?: string };
};
type ToolResult = {
	content?: Array<{ type: string; text?: string }>;
	structuredContent?: unknown;
	isError?: boolean;
};

/** Read only as far as the matching response, even if an SSE stream stays open. */
async function readResponse(res: Response, id: number): Promise<RpcMessage> {
	if (!res.headers.get("content-type")?.includes("text/event-stream")) {
		const message = await res.json() as RpcMessage;
		if (message.jsonrpc !== "2.0" || message.id !== id) {
			throw new Error("Parallel MCP returned an unexpected JSON-RPC response");
		}
		return message;
	}
	if (!res.body) throw new Error("Parallel MCP returned an empty SSE response");
	const reader = res.body.getReader();
	const decoder = new TextDecoder();
	let buffer = "";
	let data: string[] = [];
	const event = (): RpcMessage | undefined => {
		if (!data.length) return;
		const message = JSON.parse(data.join("\n")) as RpcMessage;
		data = [];
		if (message.jsonrpc === "2.0" && message.id === id) return message;
	};
	try {
		while (true) {
			const { value, done } = await reader.read();
			buffer += decoder.decode(value, { stream: !done });
			let newline: number;
			while ((newline = buffer.indexOf("\n")) >= 0) {
				const line = buffer.slice(0, newline).replace(/\r$/, "");
				buffer = buffer.slice(newline + 1);
				if (line === "") {
					const message = event();
					if (message) return message;
				} else if (line.startsWith("data:")) {
					data.push(line.slice(5).replace(/^ /, ""));
				}
			}
			if (done) {
				if (buffer.startsWith("data:")) data.push(buffer.slice(5).replace(/^ /, ""));
				const message = event();
				if (message) return message;
				throw new Error("Parallel MCP SSE stream ended without a matching response");
			}
		}
	} finally {
		await reader.cancel().catch(() => {});
		reader.releaseLock();
	}
}

async function callParallel(name: string, args: Record<string, unknown>, signal?: AbortSignal): Promise<string> {
	// One deadline covers initialization, notification, response headers and body.
	const deadline = AbortSignal.timeout(TIMEOUT_MS);
	const combined = signal ? AbortSignal.any([signal, deadline]) : deadline;
	let sessionId: string | null = null;
	let protocol = PROTOCOL;
	async function request(method: string, params?: unknown, id?: number): Promise<unknown> {
		combined.throwIfAborted();
		const headers: Record<string, string> = {
			"Content-Type": "application/json",
			Accept: "application/json, text/event-stream",
			"User-Agent": USER_AGENT,
		};
		if (method !== "initialize") headers["MCP-Protocol-Version"] = protocol;
		if (sessionId) headers["Mcp-Session-Id"] = sessionId;
		const res = await fetch(ENDPOINT, {
			method: "POST", headers, signal: combined,
			body: JSON.stringify({ jsonrpc: "2.0", ...(id === undefined ? {} : { id }), method, params }),
		});
		sessionId = res.headers.get("mcp-session-id") ?? sessionId;
		if (!res.ok) {
			const retry = res.headers.get("retry-after");
			await res.body?.cancel();
			throw new Error(`Parallel MCP HTTP ${res.status}${retry ? ` (Retry-After: ${retry})` : ""}`);
		}
		if (id === undefined) {
			await res.body?.cancel();
			return;
		}
		const message = await readResponse(res, id);
		if (message.error) throw new Error(`Parallel MCP ${message.error.code}: ${message.error.message}`);
		if (message.result === undefined) throw new Error("Parallel MCP response has no result");
		return message.result;
	}
	const initialized = await request("initialize", {
		protocolVersion: PROTOCOL, capabilities: {},
		clientInfo: { name: "pi-extensions-parallel", version: "1.0.0" },
	}, 1) as { protocolVersion?: string };
	if (initialized?.protocolVersion !== PROTOCOL) throw new Error("Parallel MCP negotiated an unsupported protocol version");
	protocol = initialized.protocolVersion;
	await request("notifications/initialized");
	const result = await request("tools/call", { name, arguments: args }, 2) as ToolResult;
	const text = result?.content?.filter((item) => item.type === "text").map((item) => item.text ?? "").join("\n").trim()
		|| (result?.structuredContent ? JSON.stringify(result.structuredContent) : "");
	if (result?.isError) throw new Error(`Parallel ${name} failed: ${text || "unknown tool error"}`);
	if (!text) throw new Error("Parallel MCP returned no text or structured content");
	return text;
}

async function execute(name: string, args: Record<string, unknown>, signal: AbortSignal | undefined, ctx: ExtensionToolContext) {
	const text = await callParallel(name, {
		...args,
		session_id: ctx.sessionManager.getSessionId(),
		...(ctx.model?.id && ctx.model.id.length <= 100 ? { model_name: ctx.model.id } : {}),
	}, signal);
	const truncated = truncateHead(text);
	let output = truncated.content;
	let fullOutputPath: string | undefined;
	if (truncated.truncated) {
		const dir = await mkdtemp(join(tmpdir(), "pi-parallel-"));
		fullOutputPath = join(dir, "result.txt");
		await writeFile(fullOutputPath, text, { mode: 0o600 });
		output += `\n\n[Output truncated. Full result: ${fullOutputPath}]`;
	}
	return { content: [{ type: "text" as const, text: output }], details: { endpoint: ENDPOINT, fullOutputPath } };
}

export default function (pi: ExtensionAPI): void {
	pi.registerTool({
		name: "parallel_search",
		label: "Parallel Search",
		description: "Search the web with Parallel for current facts and documentation. Returns source URLs and relevant excerpts. No API key needed.",
		promptSnippet: "Search current web sources with Parallel",
		promptGuidelines: ["Use parallel_search for current information outside the workspace. Answer from excerpts when sufficient; fetch pages only when more detail is needed."],
		parameters: Type.Object({
			objective: Type.String({ minLength: 1, description: "The specific information to find; may include freshness or preferred sources." }),
			search_queries: Type.Array(Type.String({ minLength: 1 }), { minItems: 1, description: "Related keyword queries, ideally 3–6 words each." }),
		}),
		async execute(_id, params, signal, _onUpdate, ctx) {
			return execute("web_search", params, signal, ctx);
		},
	});
	pi.registerTool({
		name: "parallel_fetch",
		label: "Parallel Fetch",
		description: "Extract relevant page excerpts from known HTTP/HTTPS URLs with Parallel. Use when search excerpts are insufficient or the user supplies a URL. No API key needed.",
		promptSnippet: "Read relevant excerpts from web pages with Parallel",
		parameters: Type.Object({
			urls: Type.Array(Type.String({ pattern: "^https?://", minLength: 1 }), { minItems: 1, maxItems: 20, description: "Related HTTP/HTTPS URLs to read." }),
			objective: Type.Optional(Type.String({ maxLength: 200, description: "Information to extract from these pages." })),
			search_queries: Type.Optional(Type.Array(Type.String(), { description: "Related queries from the preceding search, if any." })),
		}),
		async execute(_id, params, signal, _onUpdate, ctx) {
			return execute("web_fetch", params, signal, ctx);
		},
	});
}
