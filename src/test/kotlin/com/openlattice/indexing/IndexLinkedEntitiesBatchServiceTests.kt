package com.openlattice.indexing

import com.geekbeast.hazelcast.IHazelcastClientProvider
import com.hazelcast.core.HazelcastInstance
import com.openlattice.TestServer
import com.openlattice.data.EntityKey
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.ids.PostgresEntityKeyIdServiceTest
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.ids.HazelcastIdGenerationService
import org.apache.commons.lang3.RandomStringUtils
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.mockito.Matchers
import org.mockito.Mockito
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IndexLinkedEntitiesBatchServiceTests : TestServer() {
    companion object {
        private const val NUM_THREADS = 32
        private lateinit var postgresEntityKeyIdService: PostgresEntityKeyIdService
        private lateinit var idGenService: HazelcastIdGenerationService
        private lateinit var batchingService: IndexLinkedEntitiesBatchingService
        private val partMgr = Mockito.mock(PartitionManager::class.java)
        private val executor = Executors.newFixedThreadPool(NUM_THREADS)

        @BeforeClass
        @JvmStatic
        fun initializeServers() {

            val hzClientProvider = object : IHazelcastClientProvider {
                override fun getClient(name: String): HazelcastInstance {
                    return hazelcastInstance
                }
            }

            Mockito.`when`(partMgr.getEntitySetPartitions(UUID.randomUUID())).then {
                (0 until 257).toSet()
            }

            Mockito.doAnswer {
                val entitySetIds = it.arguments[0] as Set<UUID>
                entitySetIds.associateWith { (0 until 257).toSet() }
            }.`when`(partMgr).getPartitionsByEntitySetId(Matchers.anySet() as Set<UUID>)

            idGenService = HazelcastIdGenerationService(hzClientProvider)
            postgresEntityKeyIdService = PostgresEntityKeyIdService(
                    hds,
                    idGenService,
                    partMgr
            )
            generateIds()
            batchingService = IndexLinkedEntitiesBatchingService(hds, partMgr)
        }

        private fun generateIds() {
            val entitySetId = UUID.randomUUID()
            val expectedCount = 4096
            val entityKeys = (0 until expectedCount).map {
                EntityKey(
                        entitySetId, RandomStringUtils.randomAlphanumeric(10)
                )
            }
            val idGroups = (0 until 8)
                    .map {
                        executor.submit<MutableMap<EntityKey, UUID>> {
                            return@submit postgresEntityKeyIdService.getEntityKeyIds(entityKeys.toSet())
                        } as Future<MutableMap<EntityKey, UUID>>
                    }
                    .map { it.get() }

            val actualCount = idGroups.flatMapTo(mutableSetOf()) { it.values }.size
            Assert.assertEquals("Number of keys do not match.", expectedCount, actualCount)
        }

    }

    @Test
    fun testThings() {
        batchingService.enqueue()
        batchingService.
    }
}