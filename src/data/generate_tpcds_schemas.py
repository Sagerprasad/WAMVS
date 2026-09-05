"""
Generate PySpark StructType schemas from the canonical TPC-DS DDL.

Source:
TPC-DS Toolkit:
00_compile_tpcds/DSGen-software-code-4.0.0/tools/tpcds.sql
"""

from pathlib import Path
import re


DDL = Path(
    "/tmp/TPC-DS-Toolkit/00_compile_tpcds/"
    "DSGen-software-code-4.0.0/tools/tpcds.sql"
)

OUTPUT = Path("src/data/tpcds_schemas.py")


TYPE_MAP = {
    "integer": "IntegerType()",
    "int": "IntegerType()",
    "bigint": "LongType()",
    "smallint": "ShortType()",
    "date": "DateType()",
    "timestamp": "TimestampType()",
    "time": "StringType()",
    "varchar": "StringType()",
    "char": "StringType()",
    "string": "StringType()",
}


def spark_type(sql_type):
    sql_type = sql_type.strip().lower()

    if sql_type.startswith("decimal"):
        match = re.search(r"decimal\s*\((\d+)\s*,\s*(\d+)\)", sql_type)

        if match:
            precision = match.group(1)
            scale = match.group(2)
            return f"DecimalType({precision}, {scale})"

        return "DecimalType(18, 2)"

    for sql_name, spark_name in TYPE_MAP.items():
        if sql_type.startswith(sql_name):
            return spark_name

    raise ValueError(f"Unsupported SQL type: {sql_type}")


def parse_ddl(text):
    tables = {}

    # Capture each CREATE TABLE ... (...) block.
    pattern = re.compile(
        r"create\s+table\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\);",
        re.IGNORECASE | re.DOTALL,
    )

    for match in pattern.finditer(text):
        table_name = match.group(1).lower()
        body = match.group(2)

        fields = []

        for raw_line in body.splitlines():
            line = raw_line.strip()

            if not line:
                continue

            # Remove trailing comma.
            line = line.rstrip(",")

            # Ignore table constraints.
            lower = line.lower()

            if (
                lower.startswith("primary key")
                or lower.startswith("foreign key")
                or lower.startswith("unique")
                or lower.startswith("constraint")
            ):
                continue

            # Match:
            # column_name datatype [NOT NULL]
            field_match = re.match(
                r"^([a-zA-Z_][a-zA-Z0-9_]*)\s+"
                r"((?:decimal|numeric|varchar|char|timestamp|date|integer|"
                r"bigint|smallint|int)(?:\s*\([^)]*\))?)"
                r"(?:\s+.*)?$",
                line,
                re.IGNORECASE,
            )

            if not field_match:
                continue

            column = field_match.group(1).lower()
            sql_type = field_match.group(2)

            fields.append((column, spark_type(sql_type)))

        if fields:
            tables[table_name] = fields

    return tables


def generate(tables):
    lines = [
        '"""',
        "Auto-generated TPC-DS Spark schemas.",
        "",
        "Source: canonical TPC-DS tpcds.sql",
        "DO NOT EDIT MANUALLY.",
        '"""',
        "",
        "from pyspark.sql.types import (",
        "    StructType,",
        "    StructField,",
        "    IntegerType,",
        "    LongType,",
        "    ShortType,",
        "    DateType,",
        "    TimestampType,",
        "    StringType,",
        "    DecimalType,",
        ")",
        "",
        "",
    ]

    for table_name, fields in tables.items():
        constant = table_name.upper()

        lines.append(f"{constant}_SCHEMA = StructType([")

        for column, dtype in fields:
            lines.append(
                f'    StructField("{column}", {dtype}, True),'
            )

        lines.append("])")
        lines.append("")

    lines.append("")
    lines.append("TPCDS_SCHEMAS = {")

    for table_name in tables:
        lines.append(
            f'    "{table_name}": {table_name.upper()}_SCHEMA,'
        )

    lines.append("}")
    lines.append("")

    OUTPUT.write_text("\n".join(lines))


def main():
    if not DDL.exists():
        raise FileNotFoundError(DDL)

    text = DDL.read_text(errors="ignore")
    tables = parse_ddl(text)

    print(f"DDL source: {DDL}")
    print(f"Tables discovered: {len(tables)}")

    for table, fields in tables.items():
        print(f"  {table:25s} {len(fields):2d} columns")

    if len(tables) != 25:
        raise RuntimeError(
            f"Expected 25 TPC-DS tables, found {len(tables)}"
        )

    generate(tables)

    print()
    print(f"Generated: {OUTPUT}")


if __name__ == "__main__":
    main()
