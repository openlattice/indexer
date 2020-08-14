package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.query.Predicates
import com.openlattice.batching.AggregateBatchingService
import com.openlattice.batching.DEFAULT_BATCHING_SIZE
import com.openlattice.batching.SELECT_INSERT_CLAUSE_NAME
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.storage.EntityDatastore
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.MetadataOption
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.linking.util.PersonProperties
import com.openlattice.postgres.DataTables
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable
import com.openlattice.postgres.PostgresTable.IDS
import com.openlattice.postgres.PostgresTable.LINKING_BATCHES
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.mapstores.EntityTypeMapstore
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexLinkedEntitiesBatchingService(
        hds: HikariDataSource,
        partitionManager: PartitionManager,
        hazelcastInstance: HazelcastInstance,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: IndexingMetadataManager,
        private val dataStore: EntityDatastore,
        private val indexerConfiguration: IndexerConfiguration,
        private val batchSize: Int = DEFAULT_BATCHING_SIZE
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
        { rs -> ResultSetAdapters.linkingId(rs) }
) {
    companion object {
        private val logger = LoggerFactory.getLogger(IndexLinkedEntitiesBatchingService::class.java)
    }

    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap(hazelcastInstance)
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap(hazelcastInstance)

    // TODO if at any point there are more linkable entity types, this must change
    private val personEntityType = entityTypes.values(
            Predicates.equal(
                    EntityTypeMapstore.FULLQUALIFIED_NAME_PREDICATE,
                    PersonProperties.PERSON_TYPE_FQN.fullQualifiedNameAsString
            )
    ).first()
    private val batchType = "linked-indexing"
    // TODO if at any point there are more linkable entity types, this must change
    private val personPropertyTypes = propertyTypes.getAll(personEntityType.properties)

    /**
     *  WITH to_insert as ([SELECT_INSERT_BATCH_SQL]), marked as ([MARK_QUEUED_SQL]) [INSERT_SQL]
     *  1. LIMIT
     *  2. BATCH_TYPE
     */
    override fun bindEnqueueBatch(ps: PreparedStatement) {
        ps.setInt(1, batchSize)
        ps.setString(2, batchType)
    }

    /**
     * Binds counting query.
     * [COUNT_SQL]
     * 1. BATCH_TYPE
     */
    override fun bindQueueSize(ps: PreparedStatement) {
        ps.setString(1, batchType)
    }

    /**
     * [SELECT_PROCESS_BATCH_SQL]
     * 1. Limit
     */
    override fun bindProcessBatch(ps: PreparedStatement, partition: Int, remaining: Int) {
        ps.setInt(1, remaining)
    }

    /**
     * [SELECT_DELETE_BATCH_SQL]
     * 1. linking id
     */
    override fun bindDequeueProcessBatch(ps: PreparedStatement, batch: UUID) {
        ps.setObject(1, batch)
    }

    override fun <R> process(batch: List<UUID>): R {
        index( )
    }

    /**
     * Collect data and indexes linking ids in elasticsearch and marks them as indexed.
     * @param linkingEntityKeyIdsWithLastWrite Map of entity set id -> origin id -> linking id -> last write of linking
     * entity.
     * @param linkingIds The linking ids about to get indexed.
     */
    private fun index(
            entitySetIdsToLinkingIds: Map<UUID, Optional<Set<UUID>>>,
            linkingEntityKeyIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>
//            linkingIds: Set<UUID>
    ) {
        val linkingIds =
        logger.info("Starting background linking indexing task for linking ids $linkingIds.")
        val watch = Stopwatch.createStarted()

        // get data for linking id by entity set ids and property ids
        // (normal)entity_set_id/linking_id
        val dirtyLinkingIdsByEntitySetIds = linkingEntityKeyIdsWithLastWrite.keys.associateWith {
            Optional.of(linkingEntityKeyIdsWithLastWrite.values.flatMap { it.values.flatMap { it.keys } }.toSet())
        }
        val propertyTypesOfEntitySets = linkingEntityKeyIdsWithLastWrite.keys.associateWith { personPropertyTypes } // entity_set_id/property_type_id/property_type
        val linkedEntityData = dataStore // linking_id/(normal)entity_set_id/entity_key_id/property_type_id
                .getLinkedEntityDataByLinkingIdWithMetadata(
                        dirtyLinkingIdsByEntitySetIds,
                        propertyTypesOfEntitySets,
                        EnumSet.of(MetadataOption.LAST_WRITE)
                )

        val indexCount = indexLinkedEntities(linkedEntityData)

        logger.info(
                "Finished linked indexing $indexCount elements with linking ids $linkingIds in " +
                        "${watch.elapsed(TimeUnit.MILLISECONDS)} ms."
        )
    }

    /**
     * @param linkingIdsWithLastWrite Map of entity_set_id -> origin id -> linking_id -> last_write
     * @param dataByLinkingId Map of linking_id -> entity_set_id -> id -> property_type_id -> data
     * @return Returns the number of normal entities that are associated to the linking ids, that got indexed.
     */
    private fun indexLinkedEntities(
            dataByLinkingId: Map<UUID, Map<UUID, Map<UUID, Map<UUID, Set<Any>>>>>
    ) {
        elasticsearchApi.createBulkLinkedData(personEntityType.id, dataByLinkingId)
    }

    /**
     * @param linkingIdsWithLastWrite Map of entity_set_id -> origin_id -> linking_id -> last_write
     * @param linkingIds Set of linking_ids to delete from elasticsearch.
     * @return Returns the number of normal entities that are associated to the linking ids, that got un-indexed.
     */
    private fun unIndexLinkedEntities(
            linkingIdsWithLastWrite: Map<UUID, Map<UUID, Map<UUID, OffsetDateTime>>>,
            linkingIds: Set<UUID>
    ): Int {
        if (!elasticsearchApi.deleteEntityDataBulk(personEntityType.id, linkingIds)) {
            return 0
        }
        return dataManager.markLinkingEntitiesAsIndexed(linkingIdsWithLastWrite)
    }
}

private val NEEDS_LINKING_CLAUSES = """
        ${VERSION.name} > 0 AND
        ${DataTables.LAST_INDEX.name} >= ${LAST_WRITE.name} AND 
        ${DataTables.LAST_LINK.name} >= ${LAST_WRITE.name} AND 
        ${LAST_LINK_INDEX.name} < ${LAST_WRITE.name} 
""".trimIndent()

private val SELECT_INSERT_BATCH_SQL = """
SELECT ${LINKING_ID.name},array_agg(${PARTITION.name}) as ${PARTITION.name}, max(${LAST_WRITE.name}) as ${LAST_WRITE.name}
    FROM ${IDS.name} 
    WHERE ${LINKING_ID.name} IS NOT NULL AND $NEEDS_LINKING_CLAUSES
    GROUP BY ${LINKING_ID.name}
    LIMIT ?
""".trimIndent()

private val MARK_QUEUED_SQL = """
UPDATE ${IDS.name} SET ${LAST_LINK_INDEX.name} = now() FROM $SELECT_INSERT_CLAUSE_NAME 
    WHERE ${IDS.name}.${LINKING_ID.name} = ${SELECT_INSERT_CLAUSE_NAME}.${LINKING_ID.name} 
        AND ${IDS.name}.${PARTITION.name} = ${SELECT_INSERT_CLAUSE_NAME}.${PARTITION.name}    
""".trimIndent()

private val INSERT_SQL = """
INSERT INTO $LINKING_BATCHES SELECT *, ? as $BATCH_TYPE FROM $SELECT_INSERT_CLAUSE_NAME ON CONFLICT DO NOTHING
""".trimIndent()

private val COUNT_SQL = """
SELECT count(*) FROM ${LINKING_BATCHES.name} WHERE ${BATCH_TYPE.name} = ?
""".trimIndent()

private val SELECT_PROCESS_BATCH_SQL = """
SELECT * FROM ${LINKING_BATCHES.name}
    WHERE ${LINKING_ID.name}
    LIMIT ?
    FOR UPDATE SKIP LOCKED 
""".trimIndent()

private val SELECT_DELETE_BATCH_SQL = """
    DELETE FROM $LINKING_BATCHES WHERE ${LINKING_ID.name} = ? 
""".trimIndent()