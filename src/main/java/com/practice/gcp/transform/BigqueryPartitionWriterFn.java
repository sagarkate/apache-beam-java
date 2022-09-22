package com.practice.gcp.transform;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TimePartitioning;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.ValueInSingleWindow;

import java.time.LocalDate;

public class BigqueryPartitionWriterFn implements SerializableFunction<ValueInSingleWindow<TableRow>, TableDestination> {
    ValueProvider<String> outputTable;
    ValueProvider<String> partitionColumn;

    public BigqueryPartitionWriterFn(ValueProvider<String> outputTable, ValueProvider<String> partitionColumn) {
        this.outputTable = outputTable;
        this.partitionColumn = partitionColumn;
    }

    public Boolean isPartitionColumnDefined() {
        return !partitionColumn.get().equals("N/A");
    }

    public TableDestination apply(ValueInSingleWindow<TableRow> row) {
        TimePartitioning DAY_PARTITION = new TimePartitioning().setType("DAY");
        String destination;
        if (isPartitionColumnDefined()) {
            DAY_PARTITION.setField(partitionColumn.get());
            destination = this.outputTable.get() +
                    "$" +
                    row.getValue().get(partitionColumn.get()).toString().replaceAll("-", "");
        } else {
            destination = this.outputTable.get() +
                    "$" +
                    LocalDate.now().toString().replaceAll("-", "");
        }
        return new TableDestination(destination, null, DAY_PARTITION);
    }
}
