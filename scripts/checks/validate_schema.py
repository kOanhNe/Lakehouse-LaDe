"""Validate schema.yaml and enforce an explicit status for every declared column."""
import yaml


from lakehouse import PROJECT_ROOT as ROOT


def validate(path=ROOT / "config" / "schema.yaml"):
    schema = yaml.safe_load(path.read_text(encoding="utf-8"))
    for layer, spec in schema["layers"].items():
        names = set()
        for column in spec["columns"]:
            name = column["name"]
            if name in names:
                raise ValueError(f"Duplicate {layer} column: {name}")
            names.add(name)
            if column["status"] not in {"allowed", "forbidden", "label", "ignored"}:
                raise ValueError(f"Invalid status for {layer}.{name}")
            if column["status"] == "ignored" and not column.get("reason"):
                raise ValueError(f"Ignored column {layer}.{name} needs reason")
    roles = schema["feature_roles"]
    all_declared = {name for spec in schema["layers"].values() for item in spec["columns"]
                    for name in [item["name"]]}
    for role, columns in roles.items():
        unknown = sorted(set(columns) - all_declared)
        if unknown:
            raise ValueError(f"Unknown feature role columns in {role}: {unknown}")
        if role == "label":
            for column in columns:
                # delivery_ts is an event boundary used to decide whether a
                # row is visible; it is protected from candidate features but
                # is not itself the regression target.
                if column == "delivery_ts":
                    continue
                statuses = [item["status"] for layer in schema["layers"].values() for item in layer["columns"]
                            if item["name"] == column]
                if "label" not in statuses:
                    raise ValueError(f"Label {column} is not marked label in a layer")
    return schema


if __name__ == "__main__":
    validate()
    print("schema.yaml: OK")
