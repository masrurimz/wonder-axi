import { DateTime, Option, Result, Schema } from "effect";
import { Base64Url } from "effect/encoding";

export const REFRESH_WINDOW_MS = 5 * 60 * 1000;

const MS_PER_MINUTE = 60_000;

const MINUTES_PER_DAY = 1440;

const MINUTES_PER_HOUR = 60;

export const CredentialsSchema = Schema.Struct({
  accessToken: Schema.String,
  clientId: Schema.String,
  expiresAt: Schema.Finite,
  redirectUri: Schema.String,
  refreshToken: Schema.String,
});

export type Credentials = typeof CredentialsSchema.Type;

export const TokenResponseSchema = Schema.Struct({
  access_token: Schema.String,
  expires_in: Schema.Finite,
  id_token: Schema.optionalKey(Schema.String),
  refresh_token: Schema.optionalKey(Schema.String),
});

export type TokenResponse = typeof TokenResponseSchema.Type;

export const RegistrationResponseSchema = Schema.Struct({
  client_id: Schema.String,
});

const IdTokenClaimsSchema = Schema.Struct({
  email: Schema.optionalKey(Schema.String),
});

export const UserInfoSchema = Schema.Struct({
  email: Schema.optionalKey(Schema.String),
});

export const needsRefresh = (expiresAt: number, nowMs: number): boolean =>
  nowMs >= expiresAt - REFRESH_WINDOW_MS;

const spanLabel = (minutes: number): string => {
  const days = Math.floor(minutes / MINUTES_PER_DAY);
  const hours = Math.floor((minutes % MINUTES_PER_DAY) / MINUTES_PER_HOUR);

  if (days > 0) {
    return `${days}d ${hours}h`;
  }

  if (hours > 0) {
    return `${hours}h ${minutes % MINUTES_PER_HOUR}m`;
  }

  return `${minutes}m`;
};

export const describeExpiry = (expiresAt: number, nowMs: number) => {
  const remainingMs = expiresAt - nowMs;
  const span = spanLabel(Math.floor(Math.abs(remainingMs) / MS_PER_MINUTE));

  return {
    expiresAt: DateTime.formatIso(DateTime.makeUnsafe(expiresAt)),
    expiresIn: remainingMs >= 0 ? span : `expired ${span} ago`,
  };
};

export const credentialsFromToken = (
  previous: Pick<Credentials, "clientId" | "redirectUri"> &
    Partial<Pick<Credentials, "refreshToken">>,
  token: TokenResponse,
  nowMs: number
): Option.Option<Credentials> => {
  const refreshToken = token.refresh_token ?? previous.refreshToken;

  return refreshToken === undefined
    ? Option.none()
    : Option.some({
        accessToken: token.access_token,
        clientId: previous.clientId,
        expiresAt: nowMs + token.expires_in * 1000,
        redirectUri: previous.redirectUri,
        refreshToken,
      });
};

export const emailFromIdToken = (idToken: string): Option.Option<string> => {
  const [, payload] = idToken.split(".");

  if (payload === undefined) {
    return Option.none();
  }

  return Result.getSuccess(Base64Url.decodeString(payload)).pipe(
    Option.flatMap((json) =>
      Result.getSuccess(
        Schema.decodeUnknownResult(Schema.fromJsonString(IdTokenClaimsSchema))(
          json
        )
      )
    ),
    Option.flatMapNullishOr((claims) => claims.email)
  );
};
