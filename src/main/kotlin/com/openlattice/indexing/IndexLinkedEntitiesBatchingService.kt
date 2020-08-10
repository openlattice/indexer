package com.openlattice.indexing

import com.openlattice.batching.AggregateBatchingService
import com.openlattice.batching.SELECT_INSERT_CLAUSE_NAME
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.LINKING_BATCHES
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexLinkedEntitiesBatchingService(
        hds: HikariDataSource,
        partitionManager: PartitionManager,
        mapper: (ResultSet) -> UUID
) : AggregateBatchingService<UUID>(
        LINKING_BATCHES,
        SELECT_INSERT_BATCH_SQL,
        MARK_QUEUED_SQL,
        INSERT_SQL,
        COUNT_SQL,
        SELECT_PROCESS_BATCH_SQL,
        SELECT_DELETE_BATCH_SQL,
        hds,
        partitionManager,
        mapper
) {
    override fun bindEnqueueBatch(ps: PreparedStatement) {
        TODO("Not yet implemented")
    }

    override fun bindQueueSize(ps: PreparedStatement) {
        TODO("Not yet implemented")
    }

    override fun bindProcessBatch(ps: PreparedStatement, partition: Int, remaining: Int) {
        TODO("Not yet implemented")
    }

    override fun bindDequeueProcessBatch(ps: PreparedStatement, batch: UUID) {
        TODO("Not yet implemented")
    }
}

private val NEEDS_LINKING_CLAUSES = """
        ${VERSION.name} > 0 AND
        ${DataTables.LAST_INDEX.name} >= ${LAST_WRITE.name} AND 
        ${DataTables.LAST_LINK.name} >= ${LAST_WRITE.name} AND 
        ${PostgresColumn.LAST_LINK_INDEX.name} < ${LAST_WRITE.name} 
""".trimIndent()

private val SELECT_INSERT_BATCH_SQL = """
SELECT ${LINKING_ID.name},max(${LAST_WRITE.name}) as ${LAST_WRITE.name}
    FROM ${IDS.name} 
    WHERE ${LINKING_ID.name} IS NOT NULL AND $NEEDS_LINKING_CLAUSES
    GROUP BY ${LINKING_ID.name}
    LIMIT ?
""".trimIndent()

private val MARK_QUEUED_SQL = """
UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = ? FROM $SELECT_INSERT_CLAUSE_NAME WHERE $LINKING_ID = ${SELECT_INSERT_CLAUSE_NAME}.$LINKING_ID 
""".trimIndent()

private val INSERT_SQL = """
INSERT INTO $LINKING_BATCHES SELECT *, ? as $BATCH_TYPE FROM $SELECT_INSERT_CLAUSE_NAME ON CONFLICT DO NOTHING
""".trimIndent()

private val COUNT_SQL = """
SELECT count(*) FROM ${LINKING_BATCHES.name} WHERE ${BATCH_TYPE.name} = ?
""".trimIndent()

private val SELECT_PROCESS_BATCH_SQL = """
    
""".trimIndent()

private val SELECT_DELETE_BATCH_SQL = """
    
""".trimIndent()

private fun buildInsertBatch(idsSql: String) = """
INSERT INTO ${PostgresTable.BATCHES.name}
    SELECT ${PostgresTable.IDS.name}.${PostgresColumn.ENTITY_SET_ID.name},${PostgresTable.IDS.name}.${PostgresColumn.ID.name},${PostgresTable.IDS.name}.${PostgresColumn.LINKING_ID.name},${PostgresTable.IDS.name}.${PostgresColumn.PARTITION.name}, ? as bt
        FROM ($idsSql) as ids LEFT JOIN ${PostgresTable.BATCHES.name} USING(${PostgresColumn.ID.name},${PostgresColumn.PARTITION.name})
        WHERE ${PostgresTable.BATCHES.name}.${PostgresColumn.BATCH_TYPE.name} IS NULL
        LIMIT ?
    ON CONFLICT DO NOTHING
""".trimIndent()
