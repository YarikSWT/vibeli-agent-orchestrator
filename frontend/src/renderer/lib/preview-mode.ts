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
