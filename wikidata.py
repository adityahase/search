# Fetch and store all entities related to Jawaharlal Nehru from Wikidata
# (https://www.wikidata.org/wiki/Q1047)

from pprint import pprint

import os
import json
import requests
import itertools


# Fetch data starting from "Jawaharlal Nehru"
ROOT_ENTITIES = ["Q1047"]
LANGUAGE = "en"  # Ignore everything mentioned in other languages
WIKI_SLUG = "enwiki"  # Ignore other wikis
WIKIBASE_ITEM_DATA_TYPE = "wikibase-item"
WIKIBASE_API_MAX_IDS = 50

# Datatypes are listed at https://www.wikidata.org/wiki/Special:ListDatatypes
# Not all are useful at this point
INTERESTING_DATA_TYPES = ["time", "quantity", "string", WIKIBASE_ITEM_DATA_TYPE]

DATA_DIRECTORY = "data"


def get_entities(ids):
    """Return list of entities for given ids"""

    def fetch_batch(ids):
        # Reference: https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
        url = "https://www.wikidata.org/w/api.php"
        params = {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(ids),
            "sites": WIKI_SLUG,
            "languages": LANGUAGE,
            "sitefilter": WIKI_SLUG,
        }
        result = requests.get(url, params=params)
        store_entites_locally(result.json()["entities"].values())

    def store_entites_locally(entities):
        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)

        for entity in entities:
            file_path = os.path.join(DATA_DIRECTORY, f'{entity["id"]}.json')
            json.dump(entity, open(file_path, "w"), indent=2, sort_keys=True)

    def batches(ids):
        """Yield successive WIKIBASE_API_MAX_IDS-sized chunks from ids."""
        for ii in range(0, len(ids), WIKIBASE_API_MAX_IDS):
            yield ids[ii : ii + WIKIBASE_API_MAX_IDS]

    def get_non_local_ids(ids):
        non_local_ids = []
        for id in ids:
            file_path = os.path.join(DATA_DIRECTORY, f"{id}.json")
            if not os.path.exists(file_path):
                non_local_ids.append(id)
        return non_local_ids

    def get_local_entities(ids):
        entities = []
        for id in ids:
            file_path = os.path.join(DATA_DIRECTORY, f"{id}.json")
            entity = json.load(open(file_path))
            entities.append(entity)
        return entities

    nonlocal_ids = get_non_local_ids(ids)
    itertools.chain(*(fetch_batch(chunk) for chunk in batches(nonlocal_ids)))
    entities = get_local_entities(ids)
    return entities


class Entity:
    """Represents a Wikidata entity (Item/Property)"""

    def __init__(self, entity_id, deep=False):
        self.data = self.load_entity(entity_id)
        if deep:
            self.load_dependencies()

    def load_entity(self, entity_id):
        entity = list(get_entities([entity_id]))[0]
        return entity

    def load_dependencies(self):
        dependencies = []
        claims = self.data["claims"]
        for properties in claims.values():
            for claim in properties:
                snak = claim["mainsnak"]
                data_type = snak["datatype"]

                if data_type not in INTERESTING_DATA_TYPES:
                    continue

                dependencies.append(snak["property"])
                if data_type == WIKIBASE_ITEM_DATA_TYPE:
                    dependencies.append(snak["datavalue"]["value"]["id"])

                qualifiers = list(
                    itertools.chain(*claim.get("qualifiers", {}).values())
                )
                for qualifier in qualifiers:
                    if qualifier["snaktype"] != "value":
                        continue
                    dependencies.append(qualifier["property"])

                    if qualifier["datatype"] == WIKIBASE_ITEM_DATA_TYPE:
                        dependencies.append(qualifier["datavalue"]["value"]["id"])

        get_entities(dependencies)

    @property
    def label(self):
        return self.data["labels"].get(LANGUAGE, {}).get("value")

    @property
    def description(self):
        return self.data["descriptions"].get(LANGUAGE, {}).get("value")

    @property
    def aliases(self):
        return [alias["value"] for alias in self.data["aliases"].get(LANGUAGE, [])]

    @property
    def claims(self):
        claims = itertools.chain(*self.data["claims"].values())
        for claim in claims:
            snak = self.parse_snak(claim["mainsnak"])
            if not snak:
                continue

            qualifiers = []
            for qualifier_id in claim.get("qualifiers-order", []):
                snaks = claim["qualifiers"][qualifier_id]
                for qualifier_snak in snaks:
                    qualifier = self.parse_snak(qualifier_snak)
                    if qualifier:
                        qualifiers.append(qualifier)

            if qualifiers:
                snak = (snak[0], snak[1], qualifiers)
            yield snak

    def parse_snak(self, snak):
        # Claim:
        # Property + Value + Qualifier (Optional)
        # e.g. "instance of" "Human"
        #
        # Which is represented with symbols
        # Q1047: "Jawaharlal Nehru"
        # P31: "instance of"
        # Q5: "Human"
        #
        # {
        #     "mainsnak": {
        #         "datatype": "wikibase-item",
        #         "datavalue": {
        #             "type": "wikibase-entityid",
        #             "value": {"entity-type": "item", "id": "Q5", "numeric-id": 5},
        #         },
        #         "property": "P31",
        #         "snaktype": "value",
        #     },
        # }
        # TODO: Actual data-model is slightly more complicated, Future updates should take care of that

        # References:
        #   https://www.mediawiki.org/wiki/Wikibase/DataModel/Primer
        #   https://doc.wikimedia.org/Wikibase/master/php/md_docs_topics_json.html

        data_type = snak["datatype"]

        if data_type not in INTERESTING_DATA_TYPES:
            return None

        if snak["snaktype"] != "value":
            return

        property = Entity(snak["property"]).label
        value = snak["datavalue"]["value"]

        if data_type == WIKIBASE_ITEM_DATA_TYPE:
            value = Entity(value["id"]).label
        elif data_type == "amount":
            value = value["amount"]
        elif data_type == "time":
            value = value["time"]

        return (property, value)


for entity_id in ROOT_ENTITIES:
    entity = Entity(entity_id, deep=True)
    print("Label:", entity.label)
    print("Description:", entity.description)
    print("Aliases:", entity.aliases)
    print("Claims:")
    for claim in entity.claims:
        print(claim)
