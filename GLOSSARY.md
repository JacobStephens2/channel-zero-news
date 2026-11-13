# Glossary

**Room session**: a tab's current attempt to join one room with one role and
optional player name, together with its acknowledged identity and per-tab
recovery record; it does not own the room's authoritative game state.

**Player round participation**: one acknowledged player's assignment acquisition,
draft responses and sign-off, submission acknowledgement, and prompt-form
presentation for the room's current round; it does not own the Room session or
authoritative game state.

**Performance stage**: a tab's presentation of the room's current performance,
including slide content and index, reader/controller affordances, display stage
dismissal, sizing, and carousel input; it uses Room session identity and server
events and owns neither authoritative game state nor Player round participation.
