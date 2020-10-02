# Dmut, a tool for postgres schema migrations

Dmut helps you managing your postgres database schema code.

Unlike migration tools, which mostly consist in having sequential migrations, dmut has a notion of dependencies.
For a set of changes, dmut imposes on the user to write which previous set of changes it depends on.

Whenever a set of changes is modified, it is deapplied from the database along with all those that depended
on it prior to reapplying it with its changes.

What this allows for is agility ; no longer do you need to fiddle with faulty migrations while developping.
Just change away, and keep on working.

If a set of changes should never be touched, such as a table create statement that now has a table that
is populated, it can be locked to prevent a user from modifying it.

# TODO

 - Better check for order because it seems a little wonky right now