# Item registry listing allowed item_id values per app_id
item_registry = {
    "demo_all_inputs_v1": [
        "mood_happiness_1to5",
        "mood_sadness_1to5",
        "attention_focus_1to5"
    ],
    "nback_v2": [
        "nback_trial__rt",
        "nback_trial__correct"
    ]
    # Add more app_id and item_id lists as needed
}

# Function to check if an item_id is valid for a given app_id
def is_valid_item(app_id: str, item_id: str) -> bool:
    return item_id in item_registry.get(app_id, [])
