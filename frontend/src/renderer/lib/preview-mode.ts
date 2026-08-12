// Preview data is the fixture set the e2e suite and design previews run on. It
// used to be keyed off VITE_NO_ELECTRON, which conflated two different things:
// "this renderer has no Electron bridge" and "this renderer shows fixtures".
// A browser build serving a real daemon is the first without the second, so the
// fixture switch is its own flag and defaults to off.
export const usesPreviewWorkspaceData = import.meta.env.VITE_AO_PREVIEW_DATA === "1";

// runsWithoutElectron reports that no preload bridge exists — the renderer is
// served to a plain browser. Native-only affordances (window controls, the
// embedded browser panel) degrade; everything reachable over HTTP/WS does not.
export const runsWithoutElectron = import.meta.env.VITE_NO_ELECTRON === "1";

// externalPreviewUrl превращает внутренний адрес превью в тот, который откроется
// в браузере пользователя.
//
// Превью слушает loopback контейнера с демоном: URL из session.previewUrl
// (http://ai-conveyor-ao:3210) существует только внутри сети конвейера. Наружу
// стенд отдаётся по адресу вида https://<session>-stand.<домен>, шаблон которого
// задаётся при сборке — без него панель остаётся с прежней заглушкой.
export function externalPreviewUrl(sessionId: string | undefined, internalUrl: string): string | null {
	const template = import.meta.env.VITE_AO_PREVIEW_URL_TEMPLATE;
	if (!template || !sessionId) return null;
	const base = template.replace("{session}", sessionId).replace(/\/+$/, "");
	// Путь из внутреннего адреса сохраняется: агент мог показать не корень.
	let suffix = "";
	try {
		const parsed = new URL(internalUrl);
		suffix = `${parsed.pathname}${parsed.search}`;
	} catch {
		suffix = "";
	}
	return suffix && suffix !== "/" ? `${base}${suffix}` : base;
}
