import type { SurveyCTOCredentials, SurveyCTOAuthResponse, SurveyCTOForm } from './types';
import { apiRequest } from './http';
import { getSessionToken as getStoredSessionToken, setSessionToken } from './session';

/**
 * Authenticates with SurveyCTO server and returns available forms
 * 
 * Real implementation would call:
 * POST /api/sessions/surveycto
 * Body: { serverName, username, password }
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

  try {
    const response = await apiRequest<SurveyCTOAuthResponse>('/api/sessions/surveycto', {
      method: 'POST',
      auth: false,
      body: JSON.stringify(credentials),
    });

    if (response.success && response.sessionToken) {
      setSessionToken(response.sessionToken);
    }

    return response;
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
 * GET /api/surveycto/forms
 * Headers: { Authorization: Bearer <sessionToken> }
 */
export async function fetchForms(): Promise<SurveyCTOForm[]> {
  return apiRequest<SurveyCTOForm[]>('/api/surveycto/forms');
}

/**
 * Fetches a single form by ID with full field details
 * 
 * Real implementation would call:
 * GET /api/surveycto/forms/:formId
 */
export async function fetchFormById(formId: string): Promise<SurveyCTOForm | null> {
  return apiRequest<SurveyCTOForm | null>(`/api/surveycto/forms/${encodeURIComponent(formId)}`);
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
