package com.openlattice

import com.google.common.base.Stopwatch
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.core.IMap
import com.hazelcast.query.Predicate
import com.hazelcast.query.Predicates
import com.hazelcast.query.QueryConstants
import com.openlattice.assembler.PostgresDatabases
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.indexing.MAX_DURATION_MILLIS
import com.openlattice.indexing.configuration.IndexerConfiguration
import com.openlattice.organization.OrganizationExternalDatabaseColumn
import com.openlattice.organization.OrganizationExternalDatabaseTable
import com.openlattice.organizations.ExternalDatabaseManagementService
import com.openlattice.organizations.Organization
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.util.*
import java.util.concurrent.locks.ReentrantLock

/**
 *
 */

const val SCAN_RATE = 30_000L

class BackgroundExternalDatabaseSyncingService(
        private val hazelcastInstance: HazelcastInstance,
        private val edms: ExternalDatabaseManagementService,
        private val indexerConfiguration: IndexerConfiguration
) {
    companion object {
        private val logger = LoggerFactory.getLogger(BackgroundExternalDatabaseSyncingService::class.java)
    }

    private val organizationExternalDatabaseColumns: IMap<UUID, OrganizationExternalDatabaseColumn> = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_COLUMN.getMap( hazelcastInstance )
    private val organizationExternalDatabaseTables: IMap<UUID, OrganizationExternalDatabaseTable> = HazelcastMap.ORGANIZATION_EXTERNAL_DATABASE_TABLE.getMap( hazelcastInstance )
    private val aclKeys: IMap<String, UUID> = HazelcastMap.ACL_KEYS.getMap( hazelcastInstance )
    private val organizations: IMap<UUID, Organization> = HazelcastMap.ORGANIZATIONS.getMap( hazelcastInstance )
    private val expirationLocks: IMap<UUID, Long> = HazelcastMap.EXPIRATION_LOCKS.getMap( hazelcastInstance )


    init {
        expirationLocks.addIndex(QueryConstants.THIS_ATTRIBUTE_NAME.value(), true)
    }

    private val taskLock = ReentrantLock()

    @Suppress("UNCHECKED_CAST", "UNUSED")
    @Scheduled(fixedRate = MAX_DURATION_MILLIS)
    fun scavengeExpirationLocks() {
        expirationLocks.removeAll(
                Predicates.lessThan(
                        QueryConstants.THIS_ATTRIBUTE_NAME.value(),
                        System.currentTimeMillis()
                ) as Predicate<UUID, Long>
        )
    }

    @Suppress("UNUSED")
    @Scheduled(fixedRate = SCAN_RATE)
    fun scanOrganizationDatabases() {
        logger.info("Starting background external database sync task.")
        
        if (!indexerConfiguration.backgroundExternalDatabaseSyncingEnabled) {
            logger.info("Skipping external database syncing as it is not enabled.")
            return
        }

        if (!taskLock.tryLock()) {
            logger.info("Not starting new external database sync task as an existing one is running")
            return
        }

        try {
            val timer = Stopwatch.createStarted()
            val lockedOrganizationIds = organizations.keys
                    .filter { it != IdConstants.GLOBAL_ORGANIZATION_ID.id }
                    .filter { tryLockOrganization(it) }
                    .shuffled()

            val totalSynced = lockedOrganizationIds
                    .parallelStream()
                    .mapToInt {
                        syncOrganizationDatabases(it)
                    }
                    .sum()

            lockedOrganizationIds.forEach(this::deleteIndexingLock)

            logger.info("Completed syncing {} database objects in {}",
                    totalSynced,
                    timer)
        } finally {
            taskLock.unlock()
        }
    }

    private fun syncOrganizationDatabases(orgId: UUID): Int {
        var totalSynced = 0
        val dbName = PostgresDatabases.buildOrganizationDatabaseName(orgId)
        val currentTableIds = mutableSetOf<UUID>()
        val currentColumnIds = mutableSetOf<UUID>()
        val currentColumnNamesByTableName = edms.getColumnNamesByTable(orgId, dbName)
        currentColumnNamesByTableName.forEach { (tableName, columnNames) ->
            //check if table existed previously
            val tableFQN = FullQualifiedName(orgId.toString(), tableName)
            val tableId = aclKeys[tableFQN.fullQualifiedNameAsString]
            if (tableId == null) {
                //create new securable object for this table
                val newTable = OrganizationExternalDatabaseTable(Optional.empty(), tableName, tableName, Optional.empty(), orgId)
                val newTableId = edms.createOrganizationExternalDatabaseTable(orgId, newTable)
                currentTableIds.add(newTableId)

                //add table-level permissions
                edms.addPermissions(dbName, orgId, newTableId, newTable.name, Optional.empty(), Optional.empty())
                totalSynced++

                //create new securable objects for columns in this table
                edms.createNewColumnObjects(dbName, newTable.name, newTableId, orgId, Optional.empty())
                        .forEach { column ->
                            createNewExternalDbColumn(dbName, orgId, newTable.name, currentColumnIds, column)
                            totalSynced++
                        }
            } else {
                currentTableIds.add(tableId)
                //check if columns existed previously
                columnNames.forEach {
                    val columnFQN = FullQualifiedName(tableId.toString(), it)
                    val columnId = aclKeys[columnFQN.fullQualifiedNameAsString]
                    if (columnId == null) {
                        //create new securable object for this column
                        edms.createNewColumnObjects(dbName, tableName, tableId, orgId, Optional.of(it))
                                .forEach { column ->
                                    createNewExternalDbColumn(dbName, orgId, tableName, currentColumnIds, column)
                                    totalSynced++
                                }
                    } else {
                        currentColumnIds.add(columnId)
                    }
                }

            }

        }

        //check if tables have been deleted in the database
        val missingTableIds = organizationExternalDatabaseTables.keys - currentTableIds
        if (missingTableIds.isNotEmpty()) {
            edms.deleteOrganizationExternalDatabaseTables(orgId, missingTableIds)
            totalSynced += missingTableIds.size
        }

        //check if columns have been deleted in the database
        val missingColumnIds = organizationExternalDatabaseColumns.keys - currentColumnIds
        val missingColumnsByTable = mutableMapOf<UUID, MutableSet<UUID>>()
        if (missingColumnIds.isNotEmpty()) {
            missingColumnIds.forEach {
                val tableId = organizationExternalDatabaseColumns.getValue(it).tableId
                missingColumnsByTable.getOrPut(tableId) { mutableSetOf() }.add(it)
            }
            edms.deleteOrganizationExternalDatabaseColumns(orgId, missingColumnsByTable)
            totalSynced += missingColumnIds.size
        }
        return totalSynced
    }

    private fun tryLockOrganization(orgId: UUID): Boolean {
        return expirationLocks.putIfAbsent(orgId, System.currentTimeMillis() + MAX_DURATION_MILLIS) == null
    }

    private fun createNewExternalDbColumn(dbName: String, orgId: UUID, tableName: String, currentColumnIds: MutableSet<UUID>, column: OrganizationExternalDatabaseColumn) {
        val newColumnId = edms.createOrganizationExternalDatabaseColumn(orgId, column)
        currentColumnIds.add(newColumnId)
        edms.addPermissions(dbName, orgId, column.tableId, tableName, Optional.of(newColumnId), Optional.of(column.name))
    }

    private fun deleteIndexingLock(orgId: UUID) {
        expirationLocks.delete(orgId)
    }

}