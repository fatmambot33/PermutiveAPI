from PermutiveAPI.Import import Import,ImportList,Segment,SegmentList,Source

# Create dummy instances
import_instance = Import(id="import_id", 
                         code="import_code", 
                         name="import_name",
                         relation="relation",
                         identifiers=["identifiers"],
                         source=Source(id="SourceID",state="Act"
                         ,type="GCS"),
                         description="import_description", 
                         updated_at="import_updated_at")
import_list_instance_json=import_instance.to_json()
import_instance = Import.from_json(import_list_instance_json)
import_list_instance = ImportList(imports=[import_instance])
import_list_instance_json=import_list_instance.to_json()
import_list_instance=ImportList.from_json(import_list_instance)
print(f"import_instance::{import_instance}")
print(f"import_list_instance::{import_list_instance}")

segment_instance = Segment(code="segment_code", name="segment_name", import_id="segment_import_id")
segment_instance_instance_json=segment_instance.to_json()
segment_instance = Segment.from_json(segment_instance_instance_json)
segment_list_instance = SegmentList(segments=[segment_instance])
segment_list_instance_json=segment_list_instance.to_json()
segment_list_instance=SegmentList.from_json(segment_list_instance)
print(f"segment_instance::{segment_instance}")
print(f"segment_list_instance::{segment_list_instance}")