from dupehell.application.domain_presets import ALL_PRESETS
import json

print(f"Total domains: {len(ALL_PRESETS)}")
for name, p in list(ALL_PRESETS.items())[:3]:
    entity_names = [e["name"] for e in p.entity_plans]
    print(f"{name}: entities={entity_names}")

# Show a complete entity plan entry for KYC
kyc = ALL_PRESETS["kyc"]
for i, ep in enumerate(kyc.entity_plans):
    print(f"\nKYC entity_plan {i}:")
    print(json.dumps(ep, indent=2)[:500])

print("\nKYC hn_types:")
print(json.dumps(kyc.hn_types, indent=2)[:500])
