#!/usr/bin/env bash
# TOMBSTONE 2026-08-28. This script's contract is IMPOSSIBLE on macOS 26.5.1 and it is not
# repairable, so it fails loudly instead of failing subtly.
#
# It promised: print a code@ Apple ID verification code ON DEMAND, with no login in flight, by
# clicking "Get a Verification Code" in System Settings. Probed live on 26.5.1: that affordance
# does not exist anywhere. Not on Sign-In and Security, not on the Two-Factor Authentication
# sheet, not on this Mac's device sheet, which only states "This device is trusted and can
# receive Apple Account verification codes." An earlier note proposed fixing the OCR needle and
# navigating one level deeper; there is nothing to navigate to.
#
# Two further premises here were also wrong: System Settings is NOT accessibility-opaque on
# 26.5.1 (every row exposes a Description and an AXIdentifier, and AXPress works focuslessly, so
# the OCR-plus-synthetic-click design was never needed), and the Messages-database code path is
# not an Apple delivery channel: the only one-time-code rows there came from Tate's own number
# hand-relaying a code, which is the dependency this line of work exists to remove.
#
# WHAT TO USE INSTEAD: the code exists only while a login is actually in flight, so acquiring it
# and spending it are one operation, in one process:
#     bash /Users/ecodia/.code/ecodiaos/backend/scripts/apple-asc-login.sh
# That signs canonical Chrome into App Store Connect as code@ecodia.au with no human in the loop:
# it drives the login, presses Allow on the native trusted-device prompt over Accessibility,
# reads the 6 digits out of the AX tree, fills them, and lands on the authenticated dashboard.
# Doctrine: backend/patterns/apple-native-2fa-dialog-is-ax-drivable-2026-08-28.md
# Previous body preserved alongside this file as mint-apple-code.sh.superseded-2026-08-28.
echo "[mint-apple-code] TOMBSTONED 2026-08-28: macOS 26.5.1 has no on-demand 'Get a Verification Code' affordance." >&2
echo "[mint-apple-code] An Apple code exists ONLY during a live login. Use instead:" >&2
echo "[mint-apple-code]   bash /Users/ecodia/.code/ecodiaos/backend/scripts/apple-asc-login.sh" >&2
exit 64
