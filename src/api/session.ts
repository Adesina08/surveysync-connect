const SESSION_TOKEN_KEY = 'surveysync.sessionToken';

let inMemoryToken: string | null = null;

export function getSessionToken(): string | null {
  if (inMemoryToken) {
    return inMemoryToken;
  }

  if (typeof window === 'undefined') {
    return null;
  }

  const stored = window.localStorage.getItem(SESSION_TOKEN_KEY);
  if (stored) {
    inMemoryToken = stored;
  }
  return stored;
}

export function setSessionToken(token: string | null): void {
  inMemoryToken = token;

  if (typeof window === 'undefined') {
    return;
  }

  if (token) {
    window.localStorage.setItem(SESSION_TOKEN_KEY, token);
  } else {
    window.localStorage.removeItem(SESSION_TOKEN_KEY);
  }
}
