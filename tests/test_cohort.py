from PermutiveAPI.Cohort import Cohort, CohortList


def test_cohort_serialization():
    cohort = Cohort(
        name="C1",
        id="1",
        code="code1",
        tags=["t1"],
        workspace_id="w1",
        segment_type="type1",
    )
    json_data = cohort.to_json()
    assert json_data["name"] == "C1"
    recreated = Cohort.from_json(
        {
            "name": "C1",
            "id": "1",
            "code": "code1",
            "tags": ["t1"],
            "workspace_id": "w1",
            "segment_type": "type1",
            "created_at": cohort.created_at,
            "last_updated_at": cohort.last_updated_at,
        }
    )
    assert recreated == cohort


def test_cohort_list_caches():
    data = [
        {
            "name": "C1",
            "id": "1",
            "code": "c1",
            "tags": ["t1"],
            "segment_type": "s1",
            "workspace_id": "w1",
        },
        {
            "name": "C2",
            "id": "2",
            "code": "c2",
            "tags": ["t1", "t2"],
            "segment_type": "s2",
            "workspace_id": "w2",
        },
    ]
    cohorts = CohortList.from_json(data)
    assert cohorts.id_dictionary["1"].name == "C1"
    assert cohorts.code_dictionary["c2"].id == "2"
    assert cohorts.name_dictionary["C2"].code == "c2"
    assert cohorts.tag_dictionary["t2"][0].id == "2"
    assert cohorts.segment_type_dictionary["s1"][0].id == "1"
    assert cohorts.workspace_dictionary["w1"][0].name == "C1"
