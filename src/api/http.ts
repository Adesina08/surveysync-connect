import { getSessionToken } from './session';

type RequestOptions = RequestInit & {
  auth?: boolean;
};

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '';

function buildUrl(path: string): string {
  if (!API_BASE_URL) {
    return path;
  }
  const base = API_BASE_URL.endsWith('/') ? API_BASE_URL.slice(0, -1) : API_BASE_URL;
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return `${base}${normalizedPath}`;
}

function buildHeaders(options: RequestOptions): Headers {
  const headers = new Headers(options.headers);

  if (options.body && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json');
  }

  if (options.auth !== false) {
    const token = getSessionToken();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
  }

  return headers;
}

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const response = await fetch(buildUrl(path), {
    ...options,
    headers: buildHeaders(options),
  });

  const contentType = response.headers.get('content-type') ?? '';
  let payload: unknown = null;

  if (contentType.includes('application/json')) {
    payload = await response.json();
  } else if (response.status !== 204) {
    payload = await response.text();
  }

  if (!response.ok) {
    const messageFromPayload =
      typeof payload === 'string'
        ? payload
        : (payload as { message?: string; error?: string } | null)?.message ||
          (payload as { message?: string; error?: string } | null)?.error;
    const statusMessage = response.statusText || `HTTP ${response.status}`;
    const message = messageFromPayload || statusMessage;
    const isMissingApiBase = !API_BASE_URL && response.status === 404;
    const suggestedFix = isMissingApiBase
      ? 'API endpoint not found. Ensure the backend is running and VITE_API_BASE_URL is configured.'
      : null;
    throw new Error(suggestedFix || message || 'Request failed');
  }

  return payload as T;
}
