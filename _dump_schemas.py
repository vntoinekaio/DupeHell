import sys, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dupehell'))
from dupehell.domain.schemas import ALL_PRESETS
from dupehell.application.hn_configs import get_hn_rust_config

out_dir = os.path.join(os.path.dirname(__file__), 'schemas')
os.makedirs(out_dir, exist_ok=True)

for domain, preset in ALL_PRESETS.items():
    entities = []
    for et in preset.entity_types:
        columns_list = []
        for col in et.columns:
            col_dict = {"name": col.name, "type": col.type}
            if col.pool_name is not None:
                col_dict["pool_name"] = col.pool_name
            if not col.nullable:
                col_dict["nullable"] = False
            if col.null_rate_default > 0.0:
                col_dict["null_rate_default"] = col.null_rate_default
            if col.conditions:
                col_dict["conditions"] = [
                    {"depends_on": c.depends_on, "op": c.op, "value": c.value, "action": c.action}
                    for c in col.conditions
                ]
            columns_list.append(col_dict)

        # Get FK remaps from domain info
        domain_info = getattr(preset, 'domain_info', None)
        fk_remaps = []
        if domain_info:
            ent_info = domain_info.entity(et.name) if hasattr(domain_info, 'entity') else None
            if ent_info and hasattr(ent_info, 'foreign_keys'):
                fk_remaps = [
                    {"source_col": fk.source_col, "target_entity": fk.target_entity}
                    for fk in ent_info.foreign_keys
                ]

        entities.append({
            "name": et.name,
            "columns": columns_list,
            "fk_remaps": fk_remaps,
        })

    hn_types = []
    for h in preset.hard_negative_types:
        config_json = get_hn_rust_config(domain, h.name)
        if config_json is None:
            continue
        hn_types.append({
            "name": h.name,
            "entity_type": h.entity_type,
            "config_json": config_json,
        })

    schema = {
        "domain": domain,
        "entities": entities,
        "hn_types": hn_types,
    }
    out_path = os.path.join(out_dir, f"{domain}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"  {domain}: {len(entities)} entities, {len(hn_types)} hn types")
