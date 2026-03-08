# `.typedef` Language Reference

This document describes the syntax and semantics of `.typedef` schema files used to define messages, enums, and constants for the Coinbus platform. These schemas are compiled into Python classes by the code generator (`codegen.sh`).

## Core Concepts

### Constants

Define global constant values that can also be used as default values for message fields.

```
constant int MICROS_TO_NANOS = 1_000
```

### Enums

Define string or int-based enumerations.

```
enum str Side ->
    BUY = BUY
    SELL = SELL
```

### Messages

Define message data structures. Messages can inherit from other messages and utilize custom types as fields.

```
msg EnterOrder ->
    # Different Group Enum Reference
    common:enum.Venue venue = Venue.BINANCE

    # Same Group Enum Reference (Group name optional)
    enum.OrderType order_type = OrderType.LIMIT

    # Same Group Constant Value Reference (Group name optional)
    str internal_order_id = constant INVALID_INTERNAL_ORDER_ID

    # Custom Type
    WFloat price
```

## Supported Types

- **Primitives**: `int`, `float`, `str`, `bool`, `bytes`
- **Collections**: `List` (from `typing`, must specify the type it holds, e.g., `list[float]`)
- **Type Modifiers**: `Optional` (from `typing`)
- **Custom**: `WFloat` (for high-precision decimal arithmetic)

## Namespacing & Referencing

Types are referenced using the format `group:category.Type`:

- **Group**: The name of the `.typedef` file (e.g., `common`, `core`, `orderentry`). Optional when referencing types within the same file.
- **Category**: `enum` for enumerations, `msg` for messages.
- **Type**: The name of the defined enum or message.

### Examples

- **Cross-group type reference**: `common:enum.Venue` — References the `Venue` enum in the `common` group.
- **Same-group type reference**: `enum.OrderType` — References the `OrderType` enum in the same group (group name omitted).
- **Value references**:
  - `Venue.BINANCE` — Enum value.
  - `constant INVALID_ID` — Constant in the same group.
  - `common:constant MICROS_TO_NANOS` — Constant in a different group (group required).

## Default Values

Assigning a value to a field (e.g., `= Venue.BINANCE`) sets it as the default value. Fields without a default value are implicitly optional — they do not need to be populated when creating a message instance.

## Workflow

1. **Edit**: Modify or create a `.typedef` file.
2. **Register Group**: If adding a new group (e.g., `newgroup.typedef`), add it to the `MsgGroup` enum in `core/msg/base/msggroup.py`.
3. **Generate**: Run `sh codegen.sh` from the project root.
4. **Use**: Import the generated classes from the `generated/` directory into your application code.
