/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package com.openlattice.indexing

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.storage.IndexingMetadataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.postgres.DataTables.LAST_INDEX
import com.openlattice.postgres.DataTables.LAST_WRITE
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_KEY_IDS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.PostgresIterable
import com.openlattice.postgres.streams.StatementHolder
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Function
import java.util.function.Supplier

/**
 *
 */

const val EXPIRATION_MILLIS = 60000L
const val INDEX_RATE = 30000L
const val FETCH_SIZE = 128000

class BackgroundIndexingService(
        hazelcastInstance: HazelcastInstance,
        private val indexerConfiguration: IndexerConfiguration,
        private val hds: HikariDataSource,
        private val dataQueryService: PostgresEntityDataQueryService,
        private val elasticsearchApi: ConductorElasticsearchApi,
        private val dataManager: IndexingMetadataManager
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundIndexingService::class.java)!!
        const val INDEX_SIZE = 32000
    }

    private val propertyTypes: IMap<UUID, PropertyType> = hazelcastInstance.getMap(HazelcastMap.PROPERTY_TYPES.name)
    private val entityTypes: IMap<UUID, EntityType> = hazelcastInstance.getMap(HazelcastMap.ENTITY_TYPES.name)
    private val entitySets: IMap<UUID, EntitySet> = hazelcastInstance.getMap(HazelcastMap.ENTITY_SETS.name)
    private val indexingLocks: IMap<UUID, Long> = hazelcastInstance.getMap(HazelcastMap.INDEXING_LOCKS.name)

    init {
        indexingLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = EXPIRATION_MILLIS)
    fun scavengeIndexingLocks() {
        indexingLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = INDEX_RATE)
    fun indexUpdatedEntitySets() {
        logger.info("Starting background indexing task.")
        //Keep number of indexing jobs under control
        if (taskLock.tryLock()) {
            try {
                ensureAllEntityTypeIndicesExist()
                if (indexerConfiguration.backgroundIndexingEnabled) {
                    val w = Stopwatch.createStarted()
                    //We shuffle entity sets to make sure we have a chance to work share and index everything
                    val lockedEntitySets = entitySets.values
                            .shuffled()
                            .filter { tryLockEntitySet(it) }
                            .filter { it.name != "OpenLattice Audit Entity Set" } //TODO: Clean out audit entity set from prod

                    val totalIndexed = lockedEntitySets
                            .parallelStream()
                            .filter { !it.isLinking }
                            .mapToInt { indexEntitySet(it) }
                            .sum()

                    lockedEntitySets.forEach(this::deleteIndexingLock)

                    logger.info(
                            "Completed indexing {} elements in {} ms",
                            totalIndexed,
                            w.elapsed(TimeUnit.MILLISECONDS)
                    )
                } else {
                    logger.info("Skipping background indexing as it is not enabled.")
                }
            } finally {
                taskLock.unlock()
            }
        } else {
            logger.info("Not starting new indexing job as an existing one is running.")
        }
    }

    private fun ensureAllEntityTypeIndicesExist() {
        val existingIndices = elasticsearchApi.entityTypesWithIndices
        val missingIndices = entityTypes.keys - existingIndices
        if (missingIndices.isNotEmpty()) {
            val missingEntityTypes = entityTypes.getAll(missingIndices)
            logger.info("The following entity types were missing indices: {}", missingEntityTypes)
            missingEntityTypes.values.forEach { et ->
                val missingEntityTypePropertyTypes = propertyTypes.getAll(et.properties)
                elasticsearchApi.saveEntityTypeToElasticsearch(
                        et,
                        missingEntityTypePropertyTypes.values.toList()
                )
                logger.info("Created missing index for entity type ${et.type} with id ${et.id}")
            }
        }
    }

    private fun getEntityDataKeysQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${ENTITY_KEY_IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND ${VERSION.name} > 0"
    }

    private fun getDirtyEntitiesWithLastWriteQuery(entitySetId: UUID): String {
        return "SELECT ${ID.name}, ${LAST_WRITE.name} FROM ${ENTITY_KEY_IDS.name} " +
                "WHERE ${ENTITY_SET_ID.name} = '$entitySetId' AND " +
                "${LAST_INDEX.name} < ${LAST_WRITE.name} AND " +
                "${VERSION.name} > 0 " +
                "LIMIT $FETCH_SIZE"
    }

    private fun getEntityDataKeys(entitySetId: UUID): PostgresIterable<Pair<UUID, OffsetDateTime>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            connection.autoCommit = false
            val stmt = connection.createStatement()
            stmt.fetchSize = 64_000
            val rs = stmt.executeQuery(getEntityDataKeysQuery(entitySetId))
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, OffsetDateTime>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it)
        })
    }

    private fun getDirtyEntityKeyIds(entitySetId: UUID): PostgresIterable<Pair<UUID, OffsetDateTime>> {
        return PostgresIterable(Supplier<StatementHolder> {
            val connection = hds.connection
            val stmt = connection.createStatement()
            val rs = stmt.executeQuery(getDirtyEntitiesWithLastWriteQuery(entitySetId))
            StatementHolder(connection, stmt, rs)
        }, Function<ResultSet, Pair<UUID, OffsetDateTime>> {
            ResultSetAdapters.id(it) to ResultSetAdapters.lastWriteTyped(it)
        })
    }

    private fun getPropertyTypeForEntityType(entityTypeId: UUID): Map<UUID, PropertyType> {
        return propertyTypes
                .getAll(entityTypes[entityTypeId]?.properties ?: setOf())
                .filter { it.value.datatype != EdmPrimitiveTypeKind.Binary }
    }

    private fun indexEntitySet(entitySet: EntitySet, reindexAll: Boolean = false): Int {
        logger.info(
                "Starting indexing for entity set {} with id {}",
                entitySet.name,
                entitySet.id
        )

        val esw = Stopwatch.createStarted()
        val entityKeyIdsWithLastWrite = if (reindexAll) {
            getEntityDataKeys(entitySet.id)
        } else {
            getDirtyEntityKeyIds(entitySet.id)
        }

        val propertyTypes = getPropertyTypeForEntityType(entitySet.entityTypeId)

        var indexCount = 0
        var entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()

        while (entityKeyIdsIterator.hasNext()) {
            updateExpiration(entitySet)
            while (entityKeyIdsIterator.hasNext()) {
                val batch = getBatch(entityKeyIdsIterator)
                indexCount += indexEntities(entitySet, batch, propertyTypes, !reindexAll)
            }
            entityKeyIdsIterator = entityKeyIdsWithLastWrite.iterator()
        }

        logger.info(
                "Finished indexing {} elements from entity set {} in {} ms",
                indexCount,
                entitySet.name,
                esw.elapsed(TimeUnit.MILLISECONDS)
        )

        return indexCount
    }

    internal fun indexEntities(
            entitySet: EntitySet,
            batchToIndex: Map<UUID, OffsetDateTime>,
            propertyTypeMap: Map<UUID, PropertyType>,
            markAsIndexed: Boolean = true
    ): Int {
        val esb = Stopwatch.createStarted()
        val entitiesById = dataQueryService.getEntitiesWithPropertyTypeIds(
                mapOf(entitySet.id to Optional.of(batchToIndex.keys)),
                mapOf(entitySet.id to propertyTypeMap)).toMap()

        if (entitiesById.size != batchToIndex.size) {
            logger.error(
                    "Expected {} items to index but received {}. Marking as indexed to prevent infinite loop.",
                    batchToIndex.size,
                    entitiesById.size
            )
        }

        val indexCount: Int

        if (entitiesById.isNotEmpty() &&
                elasticsearchApi.createBulkEntityData(entitySet.entityTypeId, entitySet.id, entitiesById)) {
            indexCount = if (markAsIndexed) {
                dataManager.markAsIndexed(mapOf(entitySet.id to batchToIndex), false)
            } else {
                batchToIndex.size
            }

            logger.info(
                    "Indexed batch of {} elements for {} ({}) in {} ms",
                    indexCount,
                    entitySet.name,
                    entitySet.id,
                    esb.elapsed(TimeUnit.MILLISECONDS)
            )
        } else {
            indexCount = 0
            logger.error("Failed to index elements with entitiesById: {}", entitiesById)

        }
        return indexCount

    }

    private fun tryLockEntitySet(entitySet: EntitySet): Boolean {
        return indexingLocks.putIfAbsent(entitySet.id, System.currentTimeMillis() + EXPIRATION_MILLIS) == null
    }

    private fun deleteIndexingLock(entitySet: EntitySet) {
        indexingLocks.delete(entitySet.id)
    }

    private fun updateExpiration(entitySet: EntitySet) {
        indexingLocks.set(entitySet.id, System.currentTimeMillis() + EXPIRATION_MILLIS)
    }

    private fun getBatch(entityKeyIdStream: Iterator<Pair<UUID, OffsetDateTime>>): Map<UUID, OffsetDateTime> {
        val entityKeyIds = HashMap<UUID, OffsetDateTime>(INDEX_SIZE)

        var i = 0
        while (entityKeyIdStream.hasNext() && i < INDEX_SIZE) {
            val entityWithLastWrite = entityKeyIdStream.next()
            entityKeyIds[entityWithLastWrite.first] = entityWithLastWrite.second
            ++i
        }

        return entityKeyIds
    }
}