import type { SurveyCTOCredentials, SurveyCTOAuthResponse, SurveyCTOForm } from './types';
import { apiRequest } from './http';
import { getSessionToken as getStoredSessionToken, setSessionToken } from './session';

type SurveyCTOSessionResponse = {
  session_token: string;
  expires_at: string;
};

type SurveyCTOFormResponse = {
  form_id: string;
  title: string;
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
    name: form.title,
    version: normalizeVersion(form.version),
    responses: 0,
    lastUpdated: 'Unknown',
    fields: [],
  };
}

/**
 * Authenticates with SurveyCTO server and returns available forms
 * 
 * Real implementation would call:
 * POST /sessions
 * Body: { server_url, username, password }
 */
export async function authenticateSurveyCTO(
  credentials: SurveyCTOCredentials
): Promise<SurveyCTOAuthResponse> {
  // Validate input
  if (!credentials.serverName?.trim()) {
    return {
      success: false,
      error: 'Server name is required',
    };
  }

  if (!credentials.username?.trim()) {
    return {
      success: false,
      error: 'Username is required',
    };
  }

  if (!credentials.password?.trim()) {
    return {
      success: false,
      error: 'Password is required',
    };
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
      return {
        success: false,
        error: 'Authentication failed',
      };
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

/**
 * Fetches forms for the current session
 * 
 * Real implementation would call:
 * GET /surveycto/forms?session_token=...
 */
export async function fetchForms(): Promise<SurveyCTOForm[]> {
  const token = getStoredSessionToken();
  if (!token) {
    return [];
  }
  const forms = await apiRequest<SurveyCTOFormResponse[]>(
    `/surveycto/forms?session_token=${encodeURIComponent(token)}`
  );
  return forms.map(mapForm);
}

/**
 * Fetches a single form by ID with full field details
 * 
 * Real implementation would call:
 * GET /surveycto/forms
 */
export async function fetchFormById(formId: string): Promise<SurveyCTOForm | null> {
  const forms = await fetchForms();
  return forms.find((form) => form.id === formId) ?? null;
}

/**
 * Logs out and clears the session
 */
export function logout(): void {
  setSessionToken(null);
}

/**
 * Checks if there's an active session
 */
export function isAuthenticated(): boolean {
  return getStoredSessionToken() !== null;
}

/**
 * Gets the current session token
 */
export function getSessionToken(): string | null {
  return getStoredSessionToken();
}
