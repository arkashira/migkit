"""What the web view uses of each library it imports, and why not the
rest (backlog 0f, `tooluse`): each use read off the code that makes it,
each part left unused with its reason.
"""
from .. import tooluse

#: (library, licence, release, the files that use it,
#:  {feature: the name it is used by in them}, {feature unused: why})
LIBRARIES = (
    ("authlib", "BSD-3-Clause", "1.8.0", ("web/identity.py",),
     {"OAuth2Session": "OAuth2Session",
      "authorization URL": "create_authorization_url",
      "code exchange": "fetch_token", "PKCE": "code_challenge_method",
      "random values": "generate_token",
      "discovery document": "OpenIDProviderMetadata",
      "refresh": "refresh_token", "revocation": "revoke_token",
      "client credentials": "client_credentials",
      "authorization server": "AuthorizationServer",
      "tokens (old)": "JsonWebToken"},
     {"refresh": "a session is the view's own; when it ends the person"
                 " signs in again, so no refresh token is kept",
      "revocation": "no token of the provider's is kept past the sign-in,"
                    " so there is none to revoke",
      "client credentials": "people sign in, not machines: a machine uses"
                            " the view's own API tokens",
      "authorization server": "the view is a client of the organisation's"
                              " provider, never a provider itself",
      "tokens (old)": "its token checks are superseded by the newer library"
                      " of the same maintainer, which the view uses"}),
    ("joserfc", "BSD-3-Clause", "1.7.5", ("web/identity.py",),
     {"verify a token": "decode", "claims": "JWTClaimsRegistry",
      "key sets": "import_key_set",
      "sign a token": "serialize_compact",
      "encrypted tokens": "decrypt_compact",
      "JSON serialization": "deserialize_json"},
     {"sign a token": "the view signs no tokens: its API tokens are random"
                      " values kept as digests",
      "encrypted tokens": "an encrypted ID token is not asked for; TLS"
                          " carries it",
      "JSON serialization": "providers send ID tokens compact, and only"
                            " those are read"}),
    ("signxml", "Apache-2.0", "5.1.0", ("web/identity.py",),
     {"verify": "XMLVerifier", "what is taken": "SignatureConfiguration",
      "methods": "SignatureMethod", "sign": "XMLSigner",
      "long-term profile": "XAdESVerifier"},
     {"sign": "requests go by the redirect binding unsigned, as providers"
              " take them; the provider's answer is what is proved",
      "long-term profile": "a SAML answer carries a plain XML signature,"
                           " not the long-term profile"}),
    ("argon2-cffi", "MIT", "25.1.0", ("web/identity.py",),
     {"hasher": "PasswordHasher", "hash": "hash", "verify": "verify",
      "rehash": "check_needs_rehash", "raw hashes": "hash_secret_raw",
      "profiles": "RFC_9106_LOW_MEMORY"},
     {"raw hashes": "the hasher's own encoding keeps the parameters with"
                    " each hash",
      "profiles": "the library's default profile is used as it moves"
                  " forward; a hash made under an older one is made again"
                  " at the next sign-in"}),
    ("scim2-filter-parser", "MIT", "0.7.0", ("web/scim.py",),
     {"filters as SQL": "SQLQuery", "attributes named": "attr_paths",
      "patch paths": "AttrPath"},
     {"patch paths": "a PATCH path names one attribute or one member,"
                     " read directly"}),
    ("cheroot", "BSD-3-Clause", "11.1.2", ("web/server.py",),
     {"server": "Server", "TLS": "BuiltinSSLAdapter",
      "threads": "numthreads", "second TLS library": "PyOpenSSLAdapter",
      "peer credentials": "peercreds_enabled"},
     {"second TLS library": "the standard library's TLS is used, one"
                            " library fewer",
      "peer credentials": "the view is reached over TCP, and who asks is"
                          " the person signed in"}),
    ("werkzeug", "BSD-3-Clause", "3.1.9", ("web/app.py",),
     {"requests": "Request", "responses": "Response", "routes": "Map",
      "rules": "Rule", "forwarded headers": "ProxyFix",
      "errors": "HTTPException", "development server": "run_simple",
      "debugger": "DebuggedApplication"},
     {"development server": "a server for development; the view is served"
                            " by a production one",
      "debugger": "an interactive debugger in a served view runs code for"
                  " anyone who reaches it"}),
    ("jinja2", "BSD-3-Clause", "3.1.6", ("web/app.py",),
     {"environment": "Environment", "templates": "PackageLoader",
      "escaping": "select_autoescape", "sandbox": "SandboxedEnvironment",
      "async": "enable_async", "translations": "install_gettext_translations"},
     {"sandbox": "the templates are migkit's own, never an operator's",
      "async": "the view's server answers a request a thread",
      "translations": "the view is in English, as the rest of migkit"}),
)


def _account():
    for name, licence, release, files, features, unused in LIBRARIES:
        tooluse.register(
            name, licence=licence, release=release,
            declared={"": frozenset(features)},
            reads=(files, {("", f): t for f, t in features.items()}),
            options={("", f): (tooluse.UNUSED, why)
                     for f, why in unused.items()})


_account()
