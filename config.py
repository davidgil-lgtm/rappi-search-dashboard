# ================================================================
# Configuración de conexión a Snowflake
# No compartir este archivo
# ================================================================

SNOWFLAKE_CONFIG = {
    "account":       "hg51401",
    "user":          "DAVID.GIL@RAPPI.COM",
    "authenticator": "externalbrowser",
    "warehouse":     "RP_PERSONALUSER_WH",
    "role":          "RP_READ_ACCESS_PU_ROLE",
    "database":      "FIVETRAN",
    "schema":        "CPGS_DATASCIENCE",
}

GITHUB_REPO_PATH = "."  # ya estamos dentro de la carpeta del repo
