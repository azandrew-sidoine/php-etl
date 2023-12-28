#!/usr/bin/env sh

ssh $2@$3 /bin/sh << EOL
    mkdir ~/.etl
    cd ~/.etl
    mysqldump cnss_db employeurs assures carriere_assures --user cnssdev --port 3306 --password > employeur_assures.sql
    mysqldump cnss_db enfants assure_conjoints conjoints type_liens etat_conjoints --user cnssdev --password > beneficiaires.sql
EOL;

mkdir -p ~/.etl/db/
sftp $2@$3 /bin/sh << EOL
    cd ~/.etl
    get employeur_assures.sql ~/.etl/db/employeur_assures.sql
    get employeur_assures.sql ~/.etl/db/beneficiaires.sql
EOL;

# Execute the shell script to import data into database
echo "Importing assures & employeurs into database"
mariadb --database $MARIADB_DB --user $MARIADB_USER --port $MARIADB_PORT -p$MARIADB_PASSWORD < ~/.etl/db/employeur_assures.sql

echo "Importing conjoints & enfants into database"
mariadb --database $MARIADB_DB --user $MARIADB_USER --port $MARIADB_PORT -p$MARIADB_PASSWORD < ~/.etl/db/beneficiaires.sql

# We navigate to the directory where etl scripts are located
echo "Changing directory to $1"
cd $1

# We execute assures migration in sequence before the other 2 scripts because they need data from imported assures
echo "Migrating assures to destination database"
php migrate_assures.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT

# We execute the next 2 commands in parallel because they are not related
echo "Migrating enfants to destination database"
start=$(date +%s)
php migrate_enfants.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT  &> "./out/enfants-$start.log" &

echo "Migrating conjoints to destination database"
start=$(date +%s)
php migrate_conjoint.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT  &> "./out/conjoints-$start.log" &