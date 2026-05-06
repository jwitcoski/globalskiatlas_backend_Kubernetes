"""
Patches ski_atlas_small_medium_template.qgz:

  1. Merges downhill+snow_park difficulty rules into single difficulty:X rules
     so both piste types share the same fill colour.
  2. Removes the old generic other:snow_park rule.
  3. Adds a gladed virtual field + Gladed Runs tree overlay (exact copy of the
     natural_features_polygons RandomMarkerFill tree symbol).
  4. Adds a Snow Park Overlay: orange diagonal crosshatch on all snow_park features.
"""

import re
import shutil
import uuid
import zipfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_PATH = REPO_ROOT / "atlas/map_gen/templates/ski_atlas_small_medium_template.qgz"

# ── Exact copy of natural_features_polygons Trees/Forest marker ──────────────
TREE_DENSITY = "105"
TREE_POINT_COUNT = "16"
TREE_SEED = "610210705"
TREE_COLOR = "190,222,185,100,rgb:0.745098,0.8705882,0.7254902,0.3921569"
TREE_OUTLINE_COLOR = "61,128,53,179,rgb:0.2392157,0.5019608,0.2078431,0.7000076"
TREE_OUTLINE_WIDTH = "0.1"
TREE_SIZE = "1.3"

# ── Snow park crosshatch ──────────────────────────────────────────────────────
PARK_LINE_COLOR = "255,128,0,200,rgb:1,0.50196,0,0.78431"
PARK_LINE_WIDTH = "0.4"
PARK_LINE_SPACING = "3"

# ── Difficulty difficulties to merge ─────────────────────────────────────────
DIFFICULTIES = ["advanced", "easy", "expert", "extreme", "freeride", "intermediate", "novice"]

# These are the exact key UUIDs of the original downhill difficulty rules
# (used for targeted string replacement so labels update in-place).
DOWNHILL_RULE_KEYS = {
    "advanced":     "1c8c3a41-c53c-469a-a29c-57a8a3e23010",
    "easy":         "1944405a-a628-40bf-8c7c-9b35d5a1ad38",
    "expert":       "b9bb8bd2-d35f-4faa-a42e-1df0ea4f5ada",
    "extreme":      "d22db930-0a29-4556-b7e2-997ee0994448",
    "freeride":     "f9e4d9e8-aec1-4f3f-8cb7-b314b7b32f5c",
    "intermediate": "1c38c573-1a90-444d-ade5-bb5661d99524",
    "novice":       "6c5e87cc-aed3-4008-83e1-b22fb21f73c4",
}

# Key of the old generic snow_park rule to remove
OLD_SNOWPARK_RULE_KEY = "7113b9eb-afbf-4736-88e2-04824caa8aa1"


# ── XML helpers ──────────────────────────────────────────────────────────────

def _ddp(indent: str = "        ") -> str:
    return (
        f"{indent}<data_defined_properties>\n"
        f"{indent}  <Option type=\"Map\">\n"
        f"{indent}    <Option name=\"name\" type=\"QString\" value=\"\"/>\n"
        f"{indent}    <Option name=\"properties\"/>\n"
        f"{indent}    <Option name=\"type\" type=\"QString\" value=\"collection\"/>\n"
        f"{indent}  </Option>\n"
        f"{indent}</data_defined_properties>"
    )


def make_gladed_symbol(sym_num: int) -> str:
    rnd_id = uuid.uuid4()
    marker_id = uuid.uuid4()
    sub = f"@{sym_num}@0"
    return f"""\
      <symbol name="{sym_num}" type="fill" force_rhr="0" alpha="1" frame_rate="10" is_animated="0" clip_to_extent="1">
{_ddp("        ")}
        <layer pass="0" enabled="1" id="{{{rnd_id}}}" class="RandomMarkerFill" locked="0">
          <Option type="Map">
            <Option name="clip_points" type="QString" value="0"/>
            <Option name="count_method" type="QString" value="1"/>
            <Option name="density_area" type="QString" value="{TREE_DENSITY}"/>
            <Option name="density_area_unit" type="QString" value="MM"/>
            <Option name="density_area_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="point_count" type="QString" value="{TREE_POINT_COUNT}"/>
            <Option name="seed" type="QString" value="{TREE_SEED}"/>
          </Option>
{_ddp("          ")}
          <symbol name="{sub}" type="marker" force_rhr="0" alpha="1" frame_rate="10" is_animated="0" clip_to_extent="1">
{_ddp("            ")}
            <layer pass="0" enabled="1" id="{{{marker_id}}}" class="SimpleMarker" locked="0">
              <Option type="Map">
                <Option name="angle" type="QString" value="0"/>
                <Option name="cap_style" type="QString" value="square"/>
                <Option name="color" type="QString" value="{TREE_COLOR}"/>
                <Option name="horizontal_anchor_point" type="QString" value="1"/>
                <Option name="joinstyle" type="QString" value="bevel"/>
                <Option name="name" type="QString" value="triangle"/>
                <Option name="offset" type="QString" value="0,0"/>
                <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="offset_unit" type="QString" value="MM"/>
                <Option name="outline_color" type="QString" value="{TREE_OUTLINE_COLOR}"/>
                <Option name="outline_style" type="QString" value="solid"/>
                <Option name="outline_width" type="QString" value="{TREE_OUTLINE_WIDTH}"/>
                <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="outline_width_unit" type="QString" value="MM"/>
                <Option name="scale_method" type="QString" value="diameter"/>
                <Option name="size" type="QString" value="{TREE_SIZE}"/>
                <Option name="size_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="size_unit" type="QString" value="MM"/>
                <Option name="vertical_anchor_point" type="QString" value="1"/>
              </Option>
{_ddp("              ")}
            </layer>
          </symbol>
        </layer>
      </symbol>"""


def _line_pattern_layer(sym_num: int, sub_idx: int, angle: int) -> str:
    lpf_id = uuid.uuid4()
    line_id = uuid.uuid4()
    sub = f"@{sym_num}@{sub_idx}"
    return f"""\
        <layer pass="0" enabled="1" id="{{{lpf_id}}}" class="LinePatternFill" locked="0">
          <Option type="Map">
            <Option name="angle" type="QString" value="{angle}"/>
            <Option name="color" type="QString" value="{PARK_LINE_COLOR}"/>
            <Option name="distance" type="QString" value="{PARK_LINE_SPACING}"/>
            <Option name="distance_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="distance_unit" type="QString" value="MM"/>
            <Option name="line_width" type="QString" value="{PARK_LINE_WIDTH}"/>
            <Option name="line_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="line_width_unit" type="QString" value="MM"/>
            <Option name="offset" type="QString" value="0"/>
            <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="offset_unit" type="QString" value="MM"/>
            <Option name="outline_width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
            <Option name="outline_width_unit" type="QString" value="MM"/>
          </Option>
{_ddp("          ")}
          <symbol name="{sub}" type="line" force_rhr="0" alpha="1" frame_rate="10" is_animated="0" clip_to_extent="1">
{_ddp("            ")}
            <layer pass="0" enabled="1" id="{{{line_id}}}" class="SimpleLine" locked="0">
              <Option type="Map">
                <Option name="align_dash_pattern" type="QString" value="0"/>
                <Option name="capstyle" type="QString" value="square"/>
                <Option name="customdash" type="QString" value="5;2"/>
                <Option name="customdash_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="customdash_unit" type="QString" value="MM"/>
                <Option name="dash_pattern_offset" type="QString" value="0"/>
                <Option name="dash_pattern_offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="dash_pattern_offset_unit" type="QString" value="MM"/>
                <Option name="draw_inside_polygon" type="QString" value="0"/>
                <Option name="joinstyle" type="QString" value="bevel"/>
                <Option name="line_color" type="QString" value="{PARK_LINE_COLOR}"/>
                <Option name="line_style" type="QString" value="solid"/>
                <Option name="line_width" type="QString" value="{PARK_LINE_WIDTH}"/>
                <Option name="line_width_unit" type="QString" value="MM"/>
                <Option name="offset" type="QString" value="0"/>
                <Option name="offset_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="offset_unit" type="QString" value="MM"/>
                <Option name="ring_filter" type="QString" value="0"/>
                <Option name="trim_distance_end" type="QString" value="0"/>
                <Option name="trim_distance_end_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="trim_distance_end_unit" type="QString" value="MM"/>
                <Option name="trim_distance_start" type="QString" value="0"/>
                <Option name="trim_distance_start_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
                <Option name="trim_distance_start_unit" type="QString" value="MM"/>
                <Option name="tweak_dash_pattern_on_corners" type="QString" value="0"/>
                <Option name="use_custom_dash" type="QString" value="0"/>
                <Option name="width_map_unit_scale" type="QString" value="3x:0,0,0,0,0,0"/>
              </Option>
{_ddp("              ")}
            </layer>
          </symbol>
        </layer>"""


def make_snowpark_symbol(sym_num: int) -> str:
    return f"""\
      <symbol name="{sym_num}" type="fill" force_rhr="0" alpha="1" frame_rate="10" is_animated="0" clip_to_extent="1">
{_ddp("        ")}
{_line_pattern_layer(sym_num, 0, 45)}
{_line_pattern_layer(sym_num, 1, 135)}
      </symbol>"""


# ── Main patcher ─────────────────────────────────────────────────────────────

def patch_pistes_layer(content: str) -> str:
    layer_pattern = re.compile(
        r'(<maplayer[^>]*type="vector"[^>]*>)(.*?)(</maplayer>)',
        re.DOTALL,
    )

    def patch_block(m: re.Match) -> str:
        open_tag, body, close_tag = m.group(1), m.group(2), m.group(3)
        name_m = re.search(r'<layername>([^<]+)</layername>', body)
        if not name_m or name_m.group(1) != 'pistes_combined':
            return m.group(0)
        return open_tag + _patch_body(body) + close_tag

    return layer_pattern.sub(patch_block, content)


def _patch_body(body: str) -> str:
    # ── 1. Merge downhill+snow_park difficulty rules ──────────────────────────
    # Replace each downhill:DIFF rule filter+label with a combined difficulty:DIFF rule.
    for diff, key in DOWNHILL_RULE_KEYS.items():
        old_filter = (
            f"&quot;piste_type&quot; = 'downhill' AND"
            f" &quot;piste_diff&quot; = '{diff}'"
        )
        new_filter = (
            f"(&quot;piste_type&quot; = 'downhill' OR"
            f" &quot;piste_type&quot; = 'snow_park') AND"
            f" &quot;piste_diff&quot; = '{diff}'"
        )
        body = body.replace(
            f'filter="{old_filter}"',
            f'filter="{new_filter}"',
        )
        body = body.replace(
            f'label="downhill:{diff}"',
            f'label="difficulty:{diff}"',
        )

    # ── 2. Remove old generic other:snow_park rule ───────────────────────────
    # The rule's filter contains "&lt;>" which has a literal ">", so we
    # use [^\n]* (no newline crossing) to match the entire single-line element.
    body = re.sub(
        r'[ \t]*<rule\b[^\n]*key="\{' + OLD_SNOWPARK_RULE_KEY + r'\}"[^\n]*/>\n?',
        '',
        body,
    )

    # ── 3. Add gladed virtual expression field ────────────────────────────────
    if 'name="gladed"' not in body:
        gladed_expr = (
            "CASE WHEN &quot;other_tags&quot; ILIKE"
            " '%&quot;gladed&quot;=>&quot;yes&quot;%' THEN 'yes' END"
        )
        gladed_field = (
            f'\n        <field name="gladed" subType="0" length="0"'
            f' expression="{gladed_expr}"'
            f' typeName="" precision="0" comment="" type="10"/>'
        )
        body = re.sub(r'(</expressionfields>)', gladed_field + r'\1', body)

    # ── 4. Determine next free symbol numbers ─────────────────────────────────
    if 'Gladed Runs' in body:
        print("  Already patched — skipping overlay rules")
        return body

    sym_nums = [int(n) for n in re.findall(r'<symbol name="(\d+)"', body)]
    gladed_sym = max(sym_nums) + 1 if sym_nums else 12
    park_sym = gladed_sym + 1

    # ── 5. Inject overlay rules before </rules> ───────────────────────────────
    gladed_rule = (
        f'          <rule filter="&quot;piste_type&quot; = \'downhill\' AND'
        f' &quot;gladed&quot; = \'yes\'"'
        f' key="{{{uuid.uuid4()}}}" symbol="{gladed_sym}" label="Gladed Runs"/>'
    )
    park_rule = (
        f'          <rule filter="&quot;piste_type&quot; = \'snow_park\'"'
        f' key="{{{uuid.uuid4()}}}" symbol="{park_sym}" label="Snow Park Overlay"/>'
    )
    body = re.sub(
        r'(</rules>)',
        gladed_rule + '\n' + park_rule + '\n        ' + r'\1',
        body,
        count=1,
    )

    # ── 6. Inject symbol definitions before </symbols> ────────────────────────
    new_syms = (
        "\n" + make_gladed_symbol(gladed_sym) +
        "\n" + make_snowpark_symbol(park_sym) + "\n"
    )
    body = re.sub(r'(</symbols>)', new_syms + r'    \1', body, count=1)

    return body


def main():
    if not TEMPLATE_PATH.exists():
        print(f"Template not found: {TEMPLATE_PATH}")
        return

    backup = TEMPLATE_PATH.with_suffix('.qgz.bak')
    if backup.exists():
        shutil.copy2(backup, TEMPLATE_PATH)
        print(f"Restored clean backup: {backup.name}")
    else:
        shutil.copy2(TEMPLATE_PATH, backup)
        print(f"Backup created: {backup.name}")

    with zipfile.ZipFile(TEMPLATE_PATH, 'r') as zin:
        names = zin.namelist()
        files = {name: zin.read(name) for name in names}

    qgs_name = next(n for n in names if n.endswith('.qgs'))
    content = files[qgs_name].decode('utf-8')

    patched = patch_pistes_layer(content)

    # Verify
    checks = {
        'difficulty:advanced rule': 'difficulty:advanced' in patched,
        'downhill:advanced gone':   'downhill:advanced' not in patched,
        'snow_park:advanced gone':  'snow_park:advanced' not in patched,
        'other:snow_park gone':     OLD_SNOWPARK_RULE_KEY not in patched,
        'Gladed Runs rule':         'Gladed Runs' in patched,
        'Snow Park Overlay rule':   'Snow Park Overlay' in patched,
        'gladed expr field':        'name="gladed"' in patched,
    }
    all_ok = True
    for label, ok in checks.items():
        status = 'OK' if ok else 'FAIL'
        if not ok:
            all_ok = False
        print(f"  [{status}] {label}")

    if all_ok:
        files[qgs_name] = patched.encode('utf-8')
        with zipfile.ZipFile(TEMPLATE_PATH, 'w', zipfile.ZIP_DEFLATED) as zout:
            for name, data in files.items():
                zout.writestr(name, data)
        print(f"\nTemplate updated: {TEMPLATE_PATH.name}")
    else:
        print("\nErrors found — template NOT updated")


if __name__ == '__main__':
    main()
