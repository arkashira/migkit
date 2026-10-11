"""Huawei Cloud's API for migkit, run in the environment its SDK lives in.

The SDK pins pymongo 4.15.1 and pyasn1 0.6.3, older than migkit's own, so
it cannot share migkit's environment: `migkit doctor --install` gives it one
of its own (~/.migkit/huawei-sdk), and `cloudapi.Huawei` runs this program
there - one request as JSON on stdin, one answer as JSON on stdout, exit 1
with {"error": ...} for a refusal. It imports nothing of migkit's.

The request:

    {"service": "rds" | "dds" | "gaussdb" | "geminidb" | "iam" |
                "credentials",
     "action": the client's method (`list_instances`),
     "request": {the request model's field: value, nested as the models
                 nest},
     "region", "project_id", "domain_id", "ak", "sk", "security_token"}

A field the request model does not have is refused before anything is
sent. Without a key, the instance's own agency is asked through the SDK's
metadata provider; "credentials" answers whether it gives one.
"""
import importlib
import json
import sys

#: service -> (the SDK's module, its client, its regions)
SERVICES = {"rds": ("huaweicloudsdkrds.v3", "RdsClient",
                    "region.rds_region", "RdsRegion"),
            "dds": ("huaweicloudsdkdds.v3", "DdsClient",
                    "region.dds_region", "DdsRegion"),
            "iam": ("huaweicloudsdkiam.v3", "IamClient",
                    "region.iam_region", "IamRegion"),
            "gaussdb": ("huaweicloudsdkgaussdbforopengauss.v3",
                        "GaussDBforopenGaussClient",
                        "region.gaussdbforopengauss_region",
                        "GaussDBforopenGaussRegion"),
            "geminidb": ("huaweicloudsdkgaussdbfornosql.v3",
                         "GaussDBforNoSQLClient",
                         "region.gaussdbfornosql_region",
                         "GaussDBforNoSQLRegion")}

PLAIN = ("str", "int", "float", "bool", "object", "date", "datetime")


def model(models, kind, value):
    """`value` built as the SDK's model named `kind`, following its
    `openapi_types` through lists, maps and nested models."""
    if value is None or kind in PLAIN:
        return value
    if kind.startswith("list["):
        return [model(models, kind[5:-1], v) for v in value]
    if kind.startswith("dict("):
        inner = kind[kind.index(",") + 1:-1].strip()
        return {k: model(models, inner, v) for k, v in value.items()}
    cls = getattr(models, kind)
    unknown = sorted(set(value) - set(cls.openapi_types))
    if unknown:
        raise ValueError(f"{kind} has no {', '.join(unknown)}")
    return cls(**{k: model(models, cls.openapi_types[k], v)
                  for k, v in value.items()})


def request_class(action):
    """`list_instances` -> `ListInstancesRequest`."""
    return "".join(p.title() for p in action.split("_")) + "Request"


def credentials(ask, load):
    auth = load("huaweicloudsdkcore.auth.credentials")
    if not ask.get("ak"):
        provider = load("huaweicloudsdkcore.auth.provider") \
            .MetadataCredentialProvider
        got = (provider.get_global() if ask["service"] == "iam"
               else provider.get_basic()).get_credentials()
    elif ask["service"] == "iam":
        got = auth.GlobalCredentials(ask["ak"], ask["sk"],
                                     ask.get("domain_id"))
    else:
        got = auth.BasicCredentials(ask["ak"], ask["sk"],
                                    ask.get("project_id"))
    if ask.get("security_token"):
        got = got.with_security_token(ask["security_token"])
    if ask.get("project_id") and ask["service"] != "iam":
        got = got.with_project_id(ask["project_id"])
    return got


def answer(ask, load=importlib.import_module):
    """The answer to one request, as a dict."""
    if ask["service"] == "credentials":
        provider = load("huaweicloudsdkcore.auth.provider") \
            .MetadataCredentialProvider
        try:
            got = provider.get_basic().get_credentials()
        except Exception:  # noqa: BLE001 - no instance agency here
            got = None
        return {"found": bool(got is not None and getattr(got, "ak", None))}
    module, client, region_module, region = SERVICES[ask["service"]]
    models = load(f"{module}.model")
    regions = getattr(load(f"{module}.{region_module}"), region)
    built = getattr(load(module), client).new_builder() \
        .with_credentials(credentials(ask, load)) \
        .with_region(regions.value_of(ask["region"])).build()
    req = model(models, request_class(ask["action"]),
                ask.get("request") or {})
    return getattr(built, ask["action"])(req).to_dict()


def main():
    try:
        got = answer(json.loads(sys.stdin.read()))
    except Exception as e:  # noqa: BLE001 - said as the answer
        said = getattr(e, "error_msg", None) or str(e) or type(e).__name__
        code = getattr(e, "error_code", None)
        print(json.dumps({"error": f"{said} ({code})" if code else said}))
        return 1
    print(json.dumps(got, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
