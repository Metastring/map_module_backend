def normalize_style_colors(style_configs_data):
    """Normalize escaped hex colors in a style configs list.

    Converts strings like "\\#FFFFFF" to "#FFFFFF" for `stroke_color`
    and entries in `custom_colors`.
    """
    if not isinstance(style_configs_data, list):
        return style_configs_data

    for cfg in style_configs_data:
        if isinstance(cfg, dict):
            sc = cfg.get("stroke_color")
            if isinstance(sc, str):
                cfg["stroke_color"] = sc.replace('\\#', '#')

            cc = cfg.get("custom_colors")
            if isinstance(cc, list):
                cfg["custom_colors"] = [
                    (c.replace('\\#', '#') if isinstance(c, str) else c)
                    for c in cc
                ]

    return style_configs_data
