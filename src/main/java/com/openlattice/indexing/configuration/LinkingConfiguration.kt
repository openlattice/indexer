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

package com.openlattice.indexing.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import org.apache.olingo.commons.api.edm.FullQualifiedName

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

const val BLOCK_SIZE_FIELD = "block-size"
const val DEFAULT_BLOCK_SIZE = 10000
const val ENTITY_TYPES_FIELD = "entity-types"
private val DEFAULT_ENTITY_TYPES = setOf(FullQualifiedName("general.person"))

@ReloadableConfiguration(uri = "linking.yaml")
data class LinkingConfiguration(
        @JsonProperty(ENTITY_TYPES_FIELD) val entityTypes: Set<FullQualifiedName> = DEFAULT_ENTITY_TYPES,
        @JsonProperty(BLOCK_SIZE_FIELD) val blockSize: Int = DEFAULT_BLOCK_SIZE
) : Configuration {
    companion object {

        @JvmStatic
        @get:JvmName("key")
        val key = SimpleConfigurationKey("linking.yaml")
    }

    override fun getKey(): ConfigurationKey {
        return key
    }
}