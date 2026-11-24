"""Workload config file: declare a benchmark workload once, in JSON.

``--workload FILE`` loads the workload from a JSON file instead of
from the command line, so a benchmark can be checked in and re-run
exactly — the same file, the same workload, on any machine. The file
is an object; every key is optional and an absent key takes the
command-line default:

    {
      "ops": 20000,
      "clients": 4,
      "keys": 1000,
      "key_prefix": "kvbench",
      "value_size": 32,
      "mix": "45:40:10:5",
      "pipeline": 1,
      "distribution": "uniform",
      "zipf_skew": 1.0,
      "hot_keys": 5,
      "seed": 1234,
      "timeout": 5.0,
      "duration": "10s",
      "warmup": "2s",
      "repeat": 1,
      "sweep_keyspace": [100, 1000],
      "sweep_clients": [1, 2, 4],
      "sweep_value_size": [32, 128],
      "sweep_pipeline": [1, 5],
      "sweep_mix": ["100:0", "90:10", "50:50"]
    }

``mix`` takes the same forms as ``--mix`` (``SET:GET:INCR:DEL``
weights, a ``SET:GET:INCR`` triple, a ``READ:WRITE`` ratio, or a
preset name); ``duration`` and ``warmup`` take the same forms as
``--duration`` (``10s``, ``2m``, or a bare number of seconds — a
number is also accepted as JSON, so ``"duration": 10`` means 10
seconds). ``ops`` and ``duration`` are mutually exclusive, like the
flags. Sweep lists are JSON arrays of positive integers; they are
sorted and deduplicated, like the comma-separated flags.
``sweep_mix`` is a list of ``"READ:WRITE"`` ratio strings, parsed
like the ``--sweep-mix`` flag (read-heaviest first, duplicates
dropped).

The loader validates the file before any benchmark runs: a missing
file, invalid JSON, a non-object, an unknown key, a wrong type, or
an out-of-range value raises ValueError naming the file and what is
wrong with it, and the CLI fails with exit 2. Only the keys present
in the file are returned (normalized), so the caller can layer them
over the command-line defaults — a key the file does not mention
keeps its default instead of being reset.
"""

import json
import os


# Named read/write mix presets, as SET:GET:INCR:DEL weights. Reads are
# GETs, writes are SETs plus a small INCR share so counters still get
# exercised, and DELs are a small share of the writes — a cache
# workload deletes far less often than it reads or writes. The
# default 45:40:10:5 sits between balanced and write-heavy.
# ``--mix read-heavy`` is the same shape as ``--mix 80:20``
# (read:write), just with INCR and DEL kept in the mix.
MIX_PRESETS = {
    "read-heavy": (15, 75, 5, 5),
    "balanced": (40, 45, 10, 5),
    "write-heavy": (75, 15, 5, 5),
}


def _parse_mix(text):
    """Parse a ``--mix`` value into (set, get, incr, del, label).

    Accepts four forms:

    - ``SET:GET:INCR:DEL`` weights, e.g. ``45:40:10:5`` (label is "").
    - ``SET:GET:INCR`` weights, e.g. ``50:40:10`` — the old form,
      DEL weight 0 (label is "").
    - ``READ:WRITE`` ratio, e.g. ``80:20`` — reads are GETs, writes
      are SETs, INCR and DEL weight 0 (label is "").
    - a preset name: ``read-heavy``, ``balanced``, ``write-heavy``
      (label is the preset name, so the report can name the mix).
    """
    if text in MIX_PRESETS:
        set_w, get_w, incr_w, del_w = MIX_PRESETS[text]
        return set_w, get_w, incr_w, del_w, text
    parts = text.split(":")
    if len(parts) not in (2, 3, 4):
        raise ValueError(
            "mix must be SET:GET:INCR:DEL, SET:GET:INCR, READ:WRITE, "
            "or a preset (read-heavy, balanced, write-heavy)")
    try:
        values = [int(p) for p in parts]
    except ValueError as exc:
        raise ValueError("mix weights must be integers") from exc
    if any(v < 0 for v in values) or sum(values) <= 0:
        raise ValueError("mix weights must be non-negative and sum > 0")
    if len(values) == 2:
        # READ:WRITE — reads are GETs, writes are SETs, no INCRs
        # or DELs.
        read_w, write_w = values
        return write_w, read_w, 0, 0, ""
    if len(values) == 3:
        # SET:GET:INCR — the old form, no DELs.
        return values[0], values[1], values[2], 0, ""
    return values[0], values[1], values[2], values[3], ""


def _parse_mix_sweep(text):
    """Parse a ``--sweep-mix`` value into a list of (read, write).

    A comma-separated list of ``READ:WRITE`` ratios, e.g.
    ``100:0,90:10,50:50,10:90`` -> [(100, 0), (90, 10), (50, 50),
    (10, 90)]. Each ratio is a read:write split of the workload —
    reads are GETs, writes are SETs, with no INCRs or DELs, the
    same meaning as a two-part ``--mix`` — so a step at ``0:100``
    is a write-only run and a step at ``100:0`` a read-only one.
    Duplicates are dropped and the ratios are sorted read-heaviest
    first (by read share, then by read weight), so ``50:50,100:0``
    sweeps 100:0 then 50:50. Each part must be two non-negative
    integers summing above 0: ``0:0`` sweeps nothing, and a
    negative weight, a non-integer, a missing part or a preset
    name is rejected.
    """
    if not text:
        raise ValueError(
            "sweep-mix must list read:write ratios, "
            "e.g. 100:0,90:10,50:50,10:90")
    ratios = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValueError(
                "sweep-mix ratios must be comma-separated "
                "READ:WRITE pairs, e.g. 100:0,90:10,50:50,10:90")
        pieces = part.split(":")
        if len(pieces) != 2:
            raise ValueError(
                "sweep-mix ratios must be READ:WRITE pairs, "
                "e.g. 100:0,90:10,50:50,10:90")
        try:
            read, write = int(pieces[0]), int(pieces[1])
        except ValueError as exc:
            raise ValueError(
                "sweep-mix ratios must be integers") from exc
        if read < 0 or write < 0 or read + write <= 0:
            raise ValueError(
                "sweep-mix ratios must be non-negative and sum "
                "above 0")
        ratios.append((read, write))
    # Read-heaviest first: a step's position in the sweep reads
    # from read-only down to write-only.
    return sorted(set(ratios),
                  key=lambda ratio: (-ratio[0] / sum(ratio),
                                     -ratio[0], -ratio[1]))


def _parse_duration(text):
    """Parse a ``--duration`` value into seconds (float).

    Accepts a bare number of seconds (``10``), seconds with an
    ``s`` suffix (``10s``), or minutes with an ``m`` suffix
    (``2m``); fractional values such as ``1.5s`` or ``0.5m`` are
    allowed. The value must be positive.
    """
    if not text:
        raise ValueError(
            "duration must be a positive length of time, e.g. 10s or 2m")
    suffix = text[-1].lower()
    if suffix in ("s", "m"):
        number, multiplier = text[:-1], 60.0 if suffix == "m" else 1.0
    else:
        number, multiplier = text, 1.0
    try:
        value = float(number) * multiplier
    except ValueError as exc:
        raise ValueError(
            "duration must be a positive length of time, e.g. 10s or 2m"
        ) from exc
    if value <= 0:
        raise ValueError("duration must be positive")
    return value


# Keys the config file may set, and what each one becomes in the
# normalized dict the loader returns: (config key, normalized key).
# ``mix``, ``duration`` and ``warmup`` need their own parsing and
# are handled separately below; the sweeps are lists, not scalars.
_SCALAR_KEYS = (
    ("clients", "clients"),
    ("keys", "keys"),
    ("value_size", "value_size"),
    ("pipeline", "pipeline"),
    ("hot_keys", "hot_keys"),
    ("seed", "seed"),
    ("repeat", "repeat"),
)
_NUMBER_KEYS = (
    ("timeout", "timeout_s"),
    ("zipf_skew", "zipf_skew"),
)
_SWEEP_KEYS = (
    "sweep_keyspace",
    "sweep_clients",
    "sweep_value_size",
    "sweep_pipeline",
)
_KNOWN_KEYS = (
    {"ops", "mix", "duration", "warmup", "distribution", "key_prefix",
     "sweep_mix"}
    | {key for key, _ in _SCALAR_KEYS}
    | {key for key, _ in _NUMBER_KEYS}
    | set(_SWEEP_KEYS)
)


def _fail(path, message):
    """A ValueError naming the workload file and what is wrong."""
    raise ValueError(f"workload file {path}: {message}")


def _config_int(data, key, path, minimum=None):
    """An integer config value, or None when the key is absent.

    Booleans are rejected: JSON ``true`` is not a count. A float is
    rejected too — ``20000.0`` is a typo away from a real count, and
    the command line takes integers for these settings.
    """
    if key not in data or data[key] is None:
        return None
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(path, f"{key} must be an integer")
    if minimum is not None and value < minimum:
        _fail(path, f"{key} must be at least {minimum}")
    return value


def _config_number(data, key, path, minimum=None, minimum_exclusive=False):
    """A numeric config value as a float, or None when absent.

    Accepts an int or a float, but not a bool. ``minimum`` is
    inclusive unless ``minimum_exclusive`` is set (a duration must
    be strictly positive, a warmup may be exactly 0).
    """
    if key not in data or data[key] is None:
        return None
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, f"{key} must be a number")
    value = float(value)
    if minimum is not None:
        if minimum_exclusive and value <= minimum:
            _fail(path, f"{key} must be greater than {minimum}")
        if not minimum_exclusive and value < minimum:
            _fail(path, f"{key} must be at least {minimum}")
    return value


def _config_duration(data, key, path, allow_zero=False):
    """A duration config value in seconds, or None when absent.

    A string goes through the same parser as ``--duration``
    (``10s``, ``2m``, or a bare number of seconds); a JSON number
    is seconds directly. A duration must be positive; a warmup may
    be exactly 0 (``0`` means no warmup, the same as leaving it
    out) — the string form still requires a positive length, like
    the flag, so ``"0s"`` is rejected for both.
    """
    if key not in data or data[key] is None:
        return None
    value = data[key]
    if isinstance(value, str):
        try:
            return _parse_duration(value)
        except ValueError as exc:
            _fail(path, f"{key}: {exc}")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _fail(path, f"{key} must be a duration like \"10s\" or a "
                    "number of seconds")
    value = float(value)
    if value < 0 or (value == 0 and not allow_zero):
        if allow_zero:
            _fail(path, f"{key} must be non-negative")
        _fail(path, f"{key} must be positive")
    return value


def _config_sweep(data, key, path):
    """A sweep config value as a sorted list of distinct ints.

    Must be a JSON array of positive integers; an empty array means
    no sweep, the same as leaving the key out. Sorted and
    deduplicated, like the comma-separated sweep flags.
    """
    if key not in data or data[key] is None:
        return None
    value = data[key]
    if not isinstance(value, list):
        _fail(path, f"{key} must be a list of positive integers, "
                    "e.g. [100, 1000]")
    for size in value:
        if isinstance(size, bool) or not isinstance(size, int) \
                or size <= 0:
            _fail(path, f"{key} must be a list of positive integers, "
                        "e.g. [100, 1000]")
    return sorted(set(value))


def _config_mix_sweep(data, key, path):
    """A mix-sweep config value as a list of (read, write) tuples.

    Must be a JSON array of ``"READ:WRITE"`` ratio strings, parsed
    like the ``--sweep-mix`` flag (read-heaviest first, duplicates
    dropped); an empty array means no sweep, the same as leaving
    the key out.
    """
    if key not in data or data[key] is None:
        return None
    value = data[key]
    if not isinstance(value, list) or any(
            not isinstance(item, str) for item in value):
        _fail(path, f"{key} must be a list of READ:WRITE ratios, "
                    "e.g. [\"100:0\", \"90:10\"]")
    if not value:
        # An empty array means no sweep, the same as leaving the
        # key out — joining it would hand the parser an empty
        # string, which it (rightly) rejects.
        return []
    try:
        return _parse_mix_sweep(",".join(value))
    except ValueError as exc:
        _fail(path, f"{key}: {exc}")


def load_workload_file(path):
    """Load a workload config file into normalized settings.

    Returns a dict holding only the keys the file sets, with the
    values parsed into the shapes the CLI uses: ``mix`` is the
    ``(set, get, incr, del, label)`` tuple :func:`_parse_mix`
    returns, ``duration`` and ``warmup`` are seconds as floats
    under ``duration_s`` and ``warmup_s``, each sweep is a sorted
    list of distinct positive ints, and ``sweep_mix`` is a list of
    ``(read, write)`` tuples (see :func:`_parse_mix_sweep`). A key
    the file does not set is absent, so the caller keeps its
    default for it.

    Raises ValueError naming the file when it is missing, is not
    valid JSON, is not an object, or holds an unknown key, a wrong
    type, or an out-of-range value — including ``ops`` together
    with ``duration``, which are mutually exclusive like the flags.
    """
    if not os.path.exists(path):
        raise ValueError(f"workload file not found: {path}")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except OSError as exc:
        raise ValueError(
            f"workload file {path} cannot be read: "
            f"{exc.strerror or exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"workload file {path} is not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(
            f"workload file {path} must be a JSON object")
    unknown = sorted(set(data) - _KNOWN_KEYS)
    if unknown:
        raise ValueError(
            f"workload file {path}: unknown key(s) "
            + ", ".join(repr(k) for k in unknown)
            + " (expected "
            + ", ".join(sorted(_KNOWN_KEYS)) + ")")
    settings = {}
    ops = _config_int(data, "ops", path, minimum=1)
    if ops is not None:
        settings["ops"] = ops
    duration_s = _config_duration(data, "duration", path)
    if duration_s is not None:
        settings["duration_s"] = duration_s
    if ops is not None and duration_s is not None:
        _fail(path, "ops and duration are mutually exclusive: pick "
                    "a fixed op count or a fixed run time, not both")
    warmup_s = _config_duration(data, "warmup", path, allow_zero=True)
    if warmup_s is not None:
        settings["warmup_s"] = warmup_s
    for config_key, normalized_key in _SCALAR_KEYS:
        if config_key == "hot_keys":
            # 0 disables per-key stats; every other scalar count
            # must be at least 1 — except the seed, which the CLI
            # also leaves unrange-checked (any int seeds the run).
            minimum = 0
        elif config_key == "seed":
            minimum = None
        else:
            minimum = 1
        value = _config_int(data, config_key, path, minimum=minimum)
        if value is not None:
            settings[normalized_key] = value
    for config_key, normalized_key in _NUMBER_KEYS:
        # A timeout must be positive; a zipf skew of 0 degenerates
        # to uniform and is allowed.
        minimum = 0
        exclusive = config_key == "timeout"
        value = _config_number(data, config_key, path, minimum=minimum,
                               minimum_exclusive=exclusive)
        if value is not None:
            settings[normalized_key] = value
    if "mix" in data and data["mix"] is not None:
        if not isinstance(data["mix"], str):
            _fail(path, "mix must be a string, e.g. \"45:40:10:5\" "
                        "or \"read-heavy\"")
        try:
            settings["mix"] = _parse_mix(data["mix"])
        except ValueError as exc:
            _fail(path, f"mix: {exc}")
    if "distribution" in data and data["distribution"] is not None:
        distribution = data["distribution"]
        if distribution not in ("uniform", "zipfian"):
            _fail(path, "distribution must be 'uniform' or 'zipfian'")
        settings["distribution"] = distribution
    if "key_prefix" in data and data["key_prefix"] is not None:
        if not isinstance(data["key_prefix"], str) \
                or not data["key_prefix"]:
            _fail(path, "key_prefix must be a non-empty string")
        settings["key_prefix"] = data["key_prefix"]
    for key in _SWEEP_KEYS:
        values = _config_sweep(data, key, path)
        if values is not None:
            settings[key] = values
    mix_ratios = _config_mix_sweep(data, "sweep_mix", path)
    if mix_ratios is not None:
        settings["sweep_mix"] = mix_ratios
    return settings
