<?php

require __DIR__ . '/vendor/autoload.php';

use Drewlabs\ETL\Constraints;
use Drewlabs\ETL\ETLTask;
use Drewlabs\ETL\SQLConnectionFactory;
use Drewlabs\ETL\SQLTable;

function prepare_columns(array $columns)
{
    $values = array_keys($columns);
    $numeric = false;
    foreach ($values as $v) {
        if (is_numeric($v)) {
            $numeric = true;
            break;
        }
    }
    if ($numeric) {
        return array_reduce($columns, function ($carry, $current) {
            $col_1 = trim(str_before('<-', $current));
            $col_2 = trim(str_after('<-', $current));
            $carry[$col_1] = $col_2;
            return $carry;
        }, []);
    }

    return $columns;
}

function main(array $args)
{
    $program = 'Program: Migrate data from source to destination databases';
    printf(str_repeat('-', strlen($program)));
    printf(sprintf("\n%s\n", $program));
    printf(sprintf("%s\n", str_repeat('-', strlen($program))));
    // #region Load command line arguments and options
    // In case the list of arguments starts with - or --, the command input configuration is the last parameter, else it's the first parameter
    if (empty($args)) {
        list($optionsArgs, $argument) = [[], null];
    } else if ('-' === substr(strval($args[0]), 0, 1)) {
        // Case the total list of argument is 1 or the last element starts with - or --, we do not treat the last argument as command argument
        $optionsArgs = array_slice($args, 0, ((count($args) === 1) || ('-' === substr(strval($args[count($args) - 1]), 0, 1)) ? null : count($args) - 1));
        $argument = array_slice($args, count($optionsArgs))[0] ?? null;
    } else {
        $argument = $args[0];
        $optionsArgs = array_slice($args, 1);
    }
    $options = console_get_options($optionsArgs);
    // #endregion Load command line arguments and options
    $source = [
        "user" => $options['user'] ?? "docker",
        "password" => $options['password'] ?? "homestead",
        "host" => $options['host'] ?? '0.0.0.0',
        "db" => $options['db'] ?? "cnssdb",
        "port" => $options['port'] ?? 3306
    ];
    $destination = [
        "user" => $options['dtsUser'] ?? "docker",
        "password" => $options['dstPassword'] ?? "homestead",
        "host" => $options['dstHost'] ?? '0.0.0.0',
        "db" => $options['dstDb'] ?? "cnssdb",
        "port" => $options['dstPort'] ?? 3306
    ];

    $path = $argument ?? null;

    if (null === $argument) {
        printf("Program expect a json configuration file path as argument\n");
        // TODO: Show help
        return;
    }

    // Resolve the confuguration path relative to the base directory
    $path = resolve_path($path, $options['baseDir'] ?? __DIR__);
    $config = json_decode(file_get_contents($path), true);

    if (!is_array($config) || (is_array($config) && !isset($config['tables']))) {
        printf("Invalid configuration file, `tables` key is required in the json document \n");
        return;
    }

    $tables = (array) ($config['tables']);

    $connection = isset($config['connections']['from']) ? SQLConnectionFactory::fromArray($config['connections']['from']) : SQLConnectionFactory::fromArray($source);
    $dstConnection = isset($config['connections']['to']) ? SQLConnectionFactory::fromArray($config['connections']['to']) :  SQLConnectionFactory::fromArray($destination);

    $index = 0;

    foreach ($tables as $value) {
        if (empty($value['flow']) || empty($value['columns'])) {
            printf("Invalid configuration at index %d, `flow` and columns properties are required, stopping script...\n", $index);
            return;
        }

        $flow = strval($value['flow']);

        if (false === strpos($flow, '->')) {
            printf("Invalid flow configuration at index %d, expected `table_1 -> table_2`, got `%s`\n", $index, $flow);
        }

        // Read table 1 & and table 2 configuration from flow definition
        $table_1 = trim(str_before('->', $flow));
        $table_2 = trim(str_after('->', $flow));

        if (!is_array($value['columns'])) {
            printf("Invalid flow configuration at index %d,  columns property must be an array\n", $index);
            return;
        }

        $columns = prepare_columns($value['columns']);

        // Read table_1 column definition
        $table_1_columns = array_values(
            array_map(
                function ($column) {
                    // Replace any special character from the column name
                    return trim(str_replace(['[', ']', '(', ')', ',', ','], '', $column));
                },
                array_filter($columns, function ($column) {
                    return false !== strpos($column, '[');
                })
            )
        );

        // TODO: Add support for CSV table and connection in future release
        $table_1_connection = new SQLTable(
            isset($value['connections']['from']) ? SQLConnectionFactory::fromArray($value['connections']['from']) : $connection,
            $table_1,
            $table_1_columns
        );
        $task = new ETLTask(
            $table_1_connection,
            new SQLTable(
                isset($value['connections']['to']) ? SQLConnectionFactory::fromArray($value['connections']['to']) : $dstConnection,
                $table_2
            ),
            new Constraints($value['unique']),
            $columns,
            $value['query'] ?? null,

        );

        // Start the task
        // TODO: Use amphp/parallel to run the task in future release
        $task->run();

        $index++;
    }

    if ($index === 0) {
        printf("\nNo table loaded from configuration file, exiting...\n");
    } else {
        printf("\nImported data from %d table(s), consult your databases for more information\n", count($tables));
    }

    printf("\nThanks for using the program and providing any feedback!\n");
}

main(array_slice($argv, 1));
