import { aoBridge } from "./bridge";
import { getApiBaseUrl, hasExplicitApiBaseUrl, setApiBaseUrl, setApiDaemonStatus } from "./api-client";
import { runsWithoutElectron } from "./preview-mode";

export type DaemonStatus = Awaited<ReturnType<typeof aoBridge.daemon.getStatus>>;

export function applyDaemonStatus(nextStatus: DaemonStatus): void {
	setApiDaemonStatus(nextStatus);
	// A pinned origin wins over whatever port the daemon reports: the port is a
	// loopback fact about the machine running the daemon, and a browser on
	// another machine cannot use it.
	if (hasExplicitApiBaseUrl()) return;
	if (nextStatus.state === "ready" && nextStatus.port) {
		setApiBaseUrl(`http://127.0.0.1:${nextStatus.port}`);
	} else {
		setApiBaseUrl(null);
	}
}

export async function refreshDaemonStatus(): Promise<DaemonStatus> {
	const nextStatus = await readDaemonStatus();
	applyDaemonStatus(nextStatus);
	return nextStatus;
}

export function readDaemonStatus(): Promise<DaemonStatus> {
	// A browser build has no supervisor to ask, but the daemon itself answers
	// /readyz — and there that is the only source of truth about whether the
	// board can be filled. Keyed off the build flag rather than a missing
	// window.ao: under jsdom the bridge is absent too, and unit tests must keep
	// exercising the supervisor path.
	if (runsWithoutElectron) {
		return readDaemonStatusOverHttp();
	}
	return aoBridge.daemon.getStatus();
}

async function readDaemonStatusOverHttp(): Promise<DaemonStatus> {
	try {
		const response = await fetch(`${getApiBaseUrl()}/readyz`, { headers: { accept: "application/json" } });
		if (!response.ok) {
			return { state: "stopped", message: `daemon /readyz returned HTTP ${response.status}` };
		}
		const body = (await response.json()) as { status?: string };
		if (body.status !== "ready") {
			return { state: "starting", message: "daemon is not ready yet" };
		}
		// No port: the origin is already pinned, and reporting one would invite
		// applyDaemonStatus to repoint the client at an unreachable loopback.
		return { state: "ready" };
	} catch (error) {
		return { state: "stopped", message: error instanceof Error ? error.message : "daemon is unreachable" };
	}
}
