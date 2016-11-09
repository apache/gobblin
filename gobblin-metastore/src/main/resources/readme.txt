Database Migrations
============================
Database migrations should be contained in files within the db/migration folder.
The files should be named: Vx_x_x__Description.sql (e.g. V1_0_2__Performance_improvements.sql). When invoked,
the historystore-manager will ensure that these migrations have been applied to the target database. Any
migrations that have not been run will be run in order.

NOTE: Do NOT change previous migration files.  The checksum of each file is stored in the schema_version table
      and used as part of database validation.

For more information see: https://flywaydb.org


Job History Store Management
============================

Migrate an unmanaged database (First Time)
------------------------------------------
./historystore-manager.sh migrate -Durl="jdbc:mysql://<SERVER>/gobblin" -Duser="<USER>" -Dpassword="<PASSWORD>" -DbaselineOnMigrate=true

Migrate a database (Subsequent Time)
------------------------------------------
./historystore-manager.sh migrate -Durl="jdbc:mysql://<SERVER>/gobblin" -Duser="<USER>" -Dpassword="<PASSWORD>"

Get database migration status
------------------------------------------
./historystore-manager.sh info -Durl="jdbc:mysql://<SERVER>/gobblin" -Duser="<USER>" -Dpassword="<PASSWORD>"