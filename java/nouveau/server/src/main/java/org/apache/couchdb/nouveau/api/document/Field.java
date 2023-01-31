//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.couchdb.nouveau.api.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.dropwizard.jackson.JsonSnakeCase;

@JsonSnakeCase
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "@type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = DoubleDocValuesField.class, name = "double_dv"),
    @JsonSubTypes.Type(value = DoubleField.class, name = "double"),
    @JsonSubTypes.Type(value = DoublePoint.class, name = "double_point"),
    @JsonSubTypes.Type(value = SortedDocValuesField.class, name = "sorted_dv"),
    @JsonSubTypes.Type(value = SortedSetDocValuesField.class, name = "sorted_set_dv"),
    @JsonSubTypes.Type(value = StoredDoubleField.class, name = "stored_double"),
    @JsonSubTypes.Type(value = StoredStringField.class, name = "stored_string"),
    @JsonSubTypes.Type(value = StringField.class, name = "string"),
    @JsonSubTypes.Type(value = TextField.class, name = "text"),
})
public abstract class Field {

    protected final String name;

    protected Field(final String name) {
        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return name;
    }

}
