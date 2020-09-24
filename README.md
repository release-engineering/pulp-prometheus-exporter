This is an experimental prometheus exporter for "pulp" that just exposes information about tasks.

This polls pulp looking for tasks created since startup and exposes metrics about those tasks
for a prometheus server.

Instead of building on this, we should get a prometheus endpoint added to pulp itself, which
would be far less wasteful.