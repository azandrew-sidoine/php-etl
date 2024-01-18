#!/usr/bin/env sh
start_time=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$start_time] Migration started"
# Create a tempary mysql configuration that will be used on the remote server
cwd=$(pwd)
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

# Write MYSQL configuration file to remote server to perform a secure mysql connection
cat > my.cnf <<EOL
[client]
user=cnssdev
password=$4
EOL

# Write MYSQL configuration to a local file
CONFIG_PATH=/home/$2/.etl/my.cnf

# echo "Writing MYSQL configuration to remote server..."
sftp $2@$3 /bin/sh <<EOL
    put my.cnf $CONFIG_PATH
EOL

echo "Deleting local MYSQL configuration file..."
rm -f my.cnf

# Navigate back to the main script directory to start running migraton script
cd $cwd
echo "Authenticating to remote server to through ssh..."
ssh $2@$3 /bin/sh <<EOL
    mkdir -p /home/$2/.etl
    cd /home/$2/.etl
    mysqldump --defaults-extra-file=$CONFIG_PATH cnss_db employeurs assures carriere_assures users --user cnssdev --port 3306 > employeur_assures_users.sql
    mysqldump --defaults-extra-file=$CONFIG_PATH cnss_db enfants assure_conjoints conjoints type_liens etat_conjoints --user cnssdev --port 3306 > beneficiaires.sql
    rm -f $CONFIG_PATH
EOL

echo "SQL dump files created successfully, reading sql files using SFTP connection..."
mkdir -p ~/.etl/db/
cd ~/.etl/db/
cwd=$(pwd)
echo "Downloading sql scripts to $cwd using SFTP connection..."
sftp $2@$3 /bin/sh <<EOL
    cd /home/$2/.etl
    get employeur_assures_users.sql
    get beneficiaires.sql
EOL

echo "Files successfully downloaded using SFTP connection..."

# Execute the shell script to import data into database
echo "Importing assures & employeurs into database..."
mariadb --database $MARIADB_DB --user $MARIADB_USER --port $MARIADB_PORT -p$MARIADB_PASSWORD < $cwd/employeur_assures_users.sql

echo "Importing conjoints & enfants into database..."
mariadb --database $MARIADB_DB --user $MARIADB_USER --port $MARIADB_PORT -p$MARIADB_PASSWORD < $cwd/beneficiaires.sql

# We navigate to the directory where etl scripts are located
echo "Changing directory to $1"
cd $1

# TODO : Add source code to backup database before proceeding
cat > my-client.cnf <<EOL
[client]
user=$MARIADB_APP_USER
password=$MARIADB_APP_PASSWORD
EOL

mkdir -p ~/.etl/db/.backups
current_time=$(date +%s)

# TODO: Delete existing backups
echo "Creating database backup"
mariadb-dump --defaults-extra-file="./my-client.cnf" $MARIADB_APP_DB ass_registrants ass_registrant_contacts ass_persons ass_policy_holders ass_policy_holder_contacts ass_policy_holder_addresses ass_policy_holder_ancestors ass_registrant_policy_holders ass_mariage_bounds ass_parenting_bounds --port $MARIADB_APP_PORT > "~/.etl/db/.backups/$current_time.sql"

echo "Deleting client configuration"
rm -rf my-client.cnf

# First we migrate employeur data
echo "Migrating employeurs..."
php migrate_employeur.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT

# We execute assures migration in sequence before the other 2 scripts because they need data from imported assures
echo "Migrating assures to destination database"
php migrate_assures.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT

# We execute the next 2 commands in parallel because they are not related
echo "Migrating enfants to destination database"
start=$(date +%s)
php migrate_enfants.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT  &> "./out/enfants-$start.log" &

echo "Migrating conjoints to destination database..."
start=$(date +%s)
php migrate_conjoint.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_APP_USER --dstPassword=$MARIADB_APP_PASSWORD --dstDb=$MARIADB_APP_DB --dstPort=$MARIADB_APP_PORT  &> "./out/conjoints-$start.log" &

# echo "Migrating assures users..."
# php migrate_assure_users.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_AUTH_USER --dstPassword=$MARIADB_AUTH_PASSWORD --dstDb=$MARIADB_AUTH_DB --dstPort=$MARIADB_AUTH_PORT --appUser=$MARIADB_APP_USER --appPassword=$MARIADB_APP_PASSWORD --appDb=$MARIADB_APP_DB --appPort=$MARIADB_APP_PORT  &> "./out/migrate-ass-users-$start.log" &


#echo "Migrating employeurs users..."
#php migrate_employeur_users.php --user=$MARIADB_USER --password=$MARIADB_PASSWORD --db=$MARIADB_DB --port=$MARIADB_PORT --dstUser=$MARIADB_AUTH_USER --dstPassword=$MARIADB_AUTH_PASSWORD --dstDb=$MARIADB_AUTH_DB --dstPort=$MARIADB_AUTH_PORT --appUser=$MARIADB_APP_USER --appPassword=$MARIADB_APP_PASSWORD --appDb=$MARIADB_APP_DB --appPort=$MARIADB_APP_PORT  &> "./out/migrate-empl-users-$start.log" &


end_time=$(date '+%Y-%m-%d %H:%M:%S')
echo "[$end_time] Migration completed, check logs for any information. Thanks!"
