# OlympusPanoptes

This is intended to be integrated into DCS Olympus as an automated airborne intercept controller. The principle is that the Olympus backend exposes information about all units available in Olympus, which can be used to generate tables of the position of all red aircraft relative to each blue aircraft, which can be used to narrate picture information.

The initial planned features are:

1. Check-in by blue aircraft. Blue aircraft not checked in are ignored in order to save computational cost of computing the cartesian product of red and blue units required for the relative positional table.
2. Picture readouts relative to bulleye to be periodically broadcast over an SRS frequency.

Further features will be implemented after iteration and successful implementation of the above.
