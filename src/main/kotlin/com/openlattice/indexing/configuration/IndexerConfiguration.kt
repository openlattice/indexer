/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.Configuration
import com.kryptnostic.rhizome.configuration.ConfigurationKey
import com.kryptnostic.rhizome.configuration.SimpleConfigurationKey
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.conductor.rpc.SearchConfiguration


/**
 * In
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@ReloadableConfiguration(uri = "indexer.yaml")
data class IndexerConfiguration(
        @JsonProperty("search") val searchConfiguration: SearchConfiguration,
        @JsonProperty("error-reporting-email") val errorReportingEmail: String,
        @JsonProperty("background-indexing-enabled") val backgroundIndexingEnabled: Boolean = true
) : Configuration {
    companion object {
        @JvmStatic
        val configKey = SimpleConfigurationKey("indexer.yaml")
    }

    @JsonIgnore
    override fun getKey(): ConfigurationKey {
        return configKey
    }
}