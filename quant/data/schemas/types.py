from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union


# Primitive logical types
@dataclass(frozen=True)
class TimestampTZ:
    tz: str = "UTC"
    unit: str = "ns"  # ns for nanoseconds


@dataclass(frozen=True)
class Int64:
    pass


@dataclass(frozen=True)
class Float64:
    pass


@dataclass(frozen=True)
class String:
    pass


LogicalType = Union[TimestampTZ, Int64, Float64, String]


@dataclass(frozen=True)
class Field:
    name: str
    type: LogicalType
    nullable: bool = False


@dataclass(frozen=True)
class Schema:
    fields: List[Field]

    def field_names(self) -> List[str]:
        return [f.name for f in self.fields]

    def get_field(self, name: str) -> Optional[Field]:
        for f in self.fields:
            if f.name == name:
                return f
        return None

    def to_pyarrow(self):  # lazy conversion; import only when needed
        try:
            import pyarrow as pa  # type: ignore
        except Exception as exc:
            raise ImportError(
                "pyarrow is not installed or unavailable for this Python version"
            ) from exc

        def _to_pa_type(t: LogicalType):
            if isinstance(t, TimestampTZ):
                return pa.timestamp(t.unit, tz=t.tz)
            if isinstance(t, Int64):
                return pa.int64()
            if isinstance(t, Float64):
                return pa.float64()
            if isinstance(t, String):
                return pa.string()
            raise TypeError(f"Unsupported logical type: {t}")

        pa_fields = [pa.field(f.name, _to_pa_type(f.type), nullable=f.nullable) for f in self.fields]
        return pa.schema(pa_fields)