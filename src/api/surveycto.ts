import type { SurveyCTOCredentials, SurveyCTOAuthResponse, SurveyCTOForm } from './types';
import { mockForms, delay, shouldFail } from './mockData';

// Session storage for mock auth state
let currentSession: { token: string; serverName: string } | null = null;

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
  // Simulate network latency (800ms - 2000ms)
  await delay(800 + Math.random() * 1200);

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

  // Simulate authentication failure for specific test cases
  if (credentials.serverName.toLowerCase() === 'invalid') {
    return {
      success: false,
      error: 'Server not found. Please check the server name.',
    };
  }

  if (credentials.password === 'wrongpassword') {
    return {
      success: false,
      error: 'Invalid username or password',
    };
  }

  // Simulate random network errors (10% chance)
  if (shouldFail(0.05)) {
    return {
      success: false,
      error: 'Connection timeout. Please check your network and try again.',
    };
  }

  // Generate mock session token
  const sessionToken = `scto_${Date.now()}_${Math.random().toString(36).substring(7)}`;
  
  // Store session
  currentSession = {
    token: sessionToken,
    serverName: credentials.serverName,
  };

  return {
    success: true,
    sessionToken,
    forms: mockForms,
  };
}

/**
 * Fetches forms for the current session
 * 
 * Real implementation would call:
 * GET /api/surveycto/forms
 * Headers: { Authorization: Bearer <sessionToken> }
 */
export async function fetchForms(): Promise<SurveyCTOForm[]> {
  await delay(500 + Math.random() * 500);

  if (!currentSession) {
    throw new Error('Not authenticated. Please log in first.');
  }

  return mockForms;
}

/**
 * Fetches a single form by ID with full field details
 * 
 * Real implementation would call:
 * GET /api/surveycto/forms/:formId
 */
export async function fetchFormById(formId: string): Promise<SurveyCTOForm | null> {
  await delay(300 + Math.random() * 300);

  if (!currentSession) {
    throw new Error('Not authenticated. Please log in first.');
  }

  return mockForms.find(form => form.id === formId) || null;
}

/**
 * Logs out and clears the session
 */
export function logout(): void {
  currentSession = null;
}

/**
 * Checks if there's an active session
 */
export function isAuthenticated(): boolean {
  return currentSession !== null;
}

/**
 * Gets the current session token
 */
export function getSessionToken(): string | null {
  return currentSession?.token || null;
}
