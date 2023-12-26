<?php

namespace Drewlabs\ETL;

class ETLTask
{
    public function __construct(
        private ReadOnlyTable $from,
        private Table $to,
        private Constraints $constraints,
        private array $columns = ['*'],
        private ?string $query = null
    ) {
    }


    public function run()
    {
        $values = $this->from->all($this->query);
        $uniques = $this->constraints->getUniqueConstraints();
        // Cache is used to store possible duplicate keys for data read from
        // source table in the current process
        $cache = [];
        $data = [];
        foreach ($values as $value) {
            $attributes = [];
            $duplicated = false;
            foreach ($this->columns as $k => $prop) {
                // TODO: PROVIDES SUPPORT FOR TRANSFORM
                $result = $this->getPropertyValue($value, $prop);
                // Do an early stopping in case duplicate is detected
                if (in_array($k, $uniques)) {
                    // Case the provided colum exists in database table, we mark the record as duplicate
                    // and drop from execution context
                    if ($this->to->exists("$k = $result") || in_array(trim($result), $cache[$k] ?? [])) {
                        $duplicated = true;
                        break;
                    }
                    // We trim value that are string, because, SQL considers "A MODIFIER       " === "A MODIFIER" when 
                    // evaluating COLUMN for duplication
                    $cache[$k][] = is_numeric($result) ? $result : trim($result);
                }
                $attributes[$k] = $result;
            }

            // Case the duplicated flag value is false, we can insert
            // the column in the table, else we drop the insertion
            if (!$duplicated) {
                // Push attribute on top of the stack
                $data[] = $attributes;
            }
            // TODO: Return the list of duplicates or log them to an output file
        }
        // Case the data to be inserted is empty, we simply return
        if (empty($data)) {
            return 0;
        }

        // Return the list of added data
        return $this->to->add_many($data);
    }

    /**
     * Return the property value for a given array
     * 
     * @param array $value 
     * @param string $prop 
     * @return mixed 
     */
    private function getPropertyValue(array $value, string $prop)
    {
        return false !== strpos($prop, '[') ? ($value[trim(str_replace(['[', ']'], '', $prop))] ?? null) : (strtoupper(trim($prop)) === 'NOW()' ? date('Y-m-d H:i:s') : $prop);
    }
}
