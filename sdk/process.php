<?php

require __DIR__ . '/vendor/autoload.php';

function get_employeurs_count(\PDO $pdo)
{
    $sql = "
        SELECT COUNT(*)
        FROM employeurs
    ";
    // Create and prepare PDO statement
    $stmt = $pdo->prepare($sql);

    // Execute the PDO statement
    $stmt->execute();

    // Return the result of the query
    return $stmt->fetchColumn(0);
}

function get_employeurs(\PDO $pdo)
{
    $sql = "
        SELECT raison_sociale
        FROM employeurs
    ";
    return db_select($pdo, $sql, [], true, \PDO::FETCH_ASSOC);
}

// Function to run process in background 
function run($command, $out = '/dev/null')
{
    $pid = shell_exec(sprintf(
        '%s >> %s 2>&1 & echo $!',
        $command,
        $out
    ));

    printf("Running process %d in background: \n", $pid);

    // Return the process id
    return $pid;
}

function main(array $args)
{

    $program = 'Program: Migrate from existing source database to destination database table';
    printf(str_repeat('-', strlen($program)));
    printf("\n%s\n", $program);
    printf("%s\n", str_repeat('-', strlen($program)));
    // #region Load command line arguments and options
    // In case the list of arguments starts with - or --, the command input configuration is the last parameter, else it's the first parameter
    if (empty($args)) {
        list($optionsArgs) = [[], null];
    } else if ('-' === substr(strval($args[0]), 0, 1)) {
        // Case the total list of argument is 1 or the last element starts with - or --, we do not treat the last argument as command argument
        $optionsArgs = array_slice($args, 0, ((count($args) === 1) || ('-' === substr(strval($args[count($args) - 1]), 0, 1)) ? null : count($args) - 1));
    } else {
        $optionsArgs = array_slice($args, 1);
    }
    $options = console_get_options($optionsArgs);
    // #endregion Load command line arguments and options

    // Creates a PDO instance
    $dbUser = $options['user'] ?? "docker";
    $dbPassword = $options['password'] ?? "homestead";
    $dbHost = $options['host'] ?? '0.0.0.0';
    $dbName = $options['db'] ?? "cnssdb";
    $dbPort = $options['port'] ?? 3306;

    $dstDbUser = $options['dtsUser'] ?? "docker";
    $dstDbPassword = $options['dstPassword'] ?? "homestead";
    $dstDbHost = $options['dstHost'] ?? '0.0.0.0';
    $dstDbName = $options['dstDb'] ?? "cnssdb";
    $dstDbPort = $options['dstPort'] ?? 3306;
    $pdo = create_database_connection($dbUser, $dbPassword, $dbHost, "mysql", $dbName, $dbPort);
    // $dstPdo = create_database_connection($dstDbUser, $dstDbPassword,  $dstDbHost, "mysql", $dstDbName, $dstDbPort);

    // Set the memory limit for the current script execution
    ini_set('memory_limit', '-1');
    set_time_limit(0);
    // 
    $result = get_employeurs($pdo);
    $index = 0;
    $names = [];
    $total = get_employeurs_count($pdo);
    printf("Importing a total of %d employeurs\n", $total);
    // Create progress indicator
    $progress = console_create_progress('[%bar%] %percent%', '#', ' ', 80, $total, [
        'ansi_terminal' => true,
        'ansi_clear' => true,
    ]);
    $output = realpath(__DIR__) . DIRECTORY_SEPARATOR . "out/process_" . date('YmdHis') . ".log";
    $script = realpath(__DIR__) . DIRECTORY_SEPARATOR . 'index.php';
    $query = [];

    $start_process = function ($script, $query) use (
        $output,
        $dbUser,
        $dbPassword,
        $dbHost,
        $dbName,
        $dbPort,
        $dstDbUser,
        $dstDbPassword,
        $dstDbHost,
        $dstDbName,
        $dstDbPort
    ) {
        $command = "$(which php) " .
            escapeshellarg("$script") . " " .
            escapeshellarg("\"$query\"") . " " .
            escapeshellarg("--user=$dbUser") . " " .
            escapeshellarg("--password=$dbPassword") . " " .
            escapeshellarg("--db=$dbName") . " " .
            escapeshellarg("--host=$dbHost") . " " .
            escapeshellarg("--port=$dbPort") . " " .
            escapeshellarg("--dstUser=$dstDbUser") . " " .
            escapeshellarg("--dstPassword=$dstDbPassword") . " " .
            escapeshellarg("--dstDb=$dstDbName") . " " . //
            escapeshellarg("--dstHost=$dstDbHost") . " " .
            escapeshellarg("--dstPort=$dstDbPort") . " ";

        // Execute command in background
        run($command, $output);
    };

    foreach ($result as $value) {
        // Catch any error thrown by the update function
        $name = $value['raison_sociale'];
        if (in_array($name, $names)) {
            continue;
        }
        $names[] = $name;
        $query[] = $name;
        if (count($query) === 500) {
            $query_str = implode(',', array_unique($query));
            // Spwan PHP process
            // printf("Running shell script %s, for query %s\n", $script, $query_str);
            call_user_func_array($start_process, [$script, $query_str]);

            // Reset the query array
            $query = [];
            // Wait for 15 seconds for the next iteration
            usleep(1000 * 1000 * 15);
        }

        try {
            $progress->update($index);
        } catch(\Throwable $e) {
            // Ignore any error thrown by the progress indicator
        }

        // Increment index value
        $index++;
    }

    if (!empty($query)) {
        // TODO: Execute shell script 
        call_user_func_array($start_process, [$script, $query_str]);
        // printf("Running shell script %s, for query %s\n", $script, $query_str);
    }
}


// 
main(array_slice($argv, 1));
