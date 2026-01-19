import type { SurveyCTOCredentials, SurveyCTOAuthResponse, SurveyCTOForm } from './types';
import { apiRequest } from './http';
import { getSessionToken as getStoredSessionToken, setSessionToken } from './session';

type SurveyCTOSessionResponse = {
  session_token: string;
  expires_at: string;
};

type SurveyCTOFormResponse = {
  form_id: string;
  // SurveyCTO uses `title`, but some backends may omit it or return null.
  title?: string | null;
  version: string;
};

function normalizeServerUrl(serverName: string): string {
  let normalized = serverName.trim();
  if (!normalized) {
    return normalized;
  }

  if (!normalized.includes('.')) {
    normalized = `${normalized}.surveycto.com`;
  }

  if (!/^https?:\/\//i.test(normalized)) {
    normalized = `https://${normalized}`;
  }

  return normalized;
}

function normalizeVersion(version: string | undefined): string {
  if (!version) {
    return '1';
  }
  const trimmed = version.trim();
  if (!trimmed) {
    return '1';
  }
  return trimmed.toLowerCase().startsWith('v') ? trimmed.slice(1) || '1' : trimmed;
}

function mapForm(form: SurveyCTOFormResponse): SurveyCTOForm {
  return {
    id: form.form_id,
    // Prefer title, but fall back to id to avoid crashing the UI.
    name: (form.title ?? '').trim() || form.form_id,
    version: normalizeVersion(form.version),
    // Keep these undefined/unknown until we implement a metadata endpoint.
    responses: -1,
    lastUpdated: 'Unknown',
    // IMPORTANT: leave as undefined so the UI shows "Unknown" instead of "0 fields".
    // (The UI treats an empty array as 0.)
    fields: undefined,
  };
}

/**
 * Authenticates with SurveyCTO server and returns available forms
 */
export async function authenticateSurveyCTO(
  credentials: SurveyCTOCredentials
): Promise<SurveyCTOAuthResponse> {
  if (!credentials.serverName?.trim()) {
    return { success: false, error: 'Server name is required' };
  }
  if (!credentials.username?.trim()) {
    return { success: false, error: 'Username is required' };
  }
  if (!credentials.password?.trim()) {
    return { success: false, error: 'Password is required' };
  }

  const serverUrl = normalizeServerUrl(credentials.serverName);

  try {
    const session = await apiRequest<SurveyCTOSessionResponse>('/sessions', {
      method: 'POST',
      auth: false,
      body: JSON.stringify({
        username: credentials.username,
        password: credentials.password,
        server_url: serverUrl,
      }),
    });

    if (!session.session_token) {
      return { success: false, error: 'Authentication failed' };
    }

    setSessionToken(session.session_token);

    const forms = await apiRequest<SurveyCTOFormResponse[]>(
      `/surveycto/forms?session_token=${encodeURIComponent(session.session_token)}`
    );

    return {
      success: true,
      sessionToken: session.session_token,
      forms: forms.map(mapForm),
    };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Authentication failed',
    };
  }
}

export async function fetchForms(): Promise<SurveyCTOForm[]> {
  const token = getStoredSessionToken();
  if (!token) return [];
  const forms = await apiRequest<SurveyCTOFormResponse[]>(
    `/surveycto/forms?session_token=${encodeURIComponent(token)}`
  );
  return forms.map(mapForm);
}

export async function fetchFormById(formId: string): Promise<SurveyCTOForm | null> {
  const forms = await fetchForms();
  return forms.find((form) => form.id === formId) ?? null;
}

export function logout(): void {
  setSessionToken(null);
}

export function isAuthenticated(): boolean {
  return getStoredSessionToken() !== null;
}

export function getSessionToken(): string | null {
  return getStoredSessionToken();
}
