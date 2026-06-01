//! Catalog relation OIDs, extracted 1:1 from the `CATALOG(name, oid, SymbolId)`
//! macros in postgres/src/include/catalog/*.h (the symbol + OID the C build's
//! genbki assigns to each system catalog's pg_class entry).
//!
//! e.g. TypeRelationId = 1247, RelationRelationId = 1259, ProcedureRelationId = 1255.

use crate::postgres_ext::Oid;

pub const DefaultAclRelationId: Oid = 826; // pg_default_acl
pub const TableSpaceRelationId: Oid = 1213; // pg_tablespace
pub const SharedDependRelationId: Oid = 1214; // pg_shdepend
pub const TypeRelationId: Oid = 1247; // pg_type
pub const AttributeRelationId: Oid = 1249; // pg_attribute
pub const ProcedureRelationId: Oid = 1255; // pg_proc
pub const RelationRelationId: Oid = 1259; // pg_class
pub const AuthIdRelationId: Oid = 1260; // pg_authid
pub const AuthMemRelationId: Oid = 1261; // pg_auth_members
pub const DatabaseRelationId: Oid = 1262; // pg_database
pub const ForeignServerRelationId: Oid = 1417; // pg_foreign_server
pub const UserMappingRelationId: Oid = 1418; // pg_user_mapping
pub const SequenceRelationId: Oid = 2224; // pg_sequence
pub const ForeignDataWrapperRelationId: Oid = 2328; // pg_foreign_data_wrapper
pub const SharedDescriptionRelationId: Oid = 2396; // pg_shdescription
pub const AggregateRelationId: Oid = 2600; // pg_aggregate
pub const AccessMethodRelationId: Oid = 2601; // pg_am
pub const AccessMethodOperatorRelationId: Oid = 2602; // pg_amop
pub const AccessMethodProcedureRelationId: Oid = 2603; // pg_amproc
pub const AttrDefaultRelationId: Oid = 2604; // pg_attrdef
pub const CastRelationId: Oid = 2605; // pg_cast
pub const ConstraintRelationId: Oid = 2606; // pg_constraint
pub const ConversionRelationId: Oid = 2607; // pg_conversion
pub const DependRelationId: Oid = 2608; // pg_depend
pub const DescriptionRelationId: Oid = 2609; // pg_description
pub const IndexRelationId: Oid = 2610; // pg_index
pub const InheritsRelationId: Oid = 2611; // pg_inherits
pub const LanguageRelationId: Oid = 2612; // pg_language
pub const LargeObjectRelationId: Oid = 2613; // pg_largeobject
pub const NamespaceRelationId: Oid = 2615; // pg_namespace
pub const OperatorClassRelationId: Oid = 2616; // pg_opclass
pub const OperatorRelationId: Oid = 2617; // pg_operator
pub const RewriteRelationId: Oid = 2618; // pg_rewrite
pub const StatisticRelationId: Oid = 2619; // pg_statistic
pub const TriggerRelationId: Oid = 2620; // pg_trigger
pub const OperatorFamilyRelationId: Oid = 2753; // pg_opfamily
pub const DbRoleSettingRelationId: Oid = 2964; // pg_db_role_setting
pub const LargeObjectMetadataRelationId: Oid = 2995; // pg_largeobject_metadata
pub const ExtensionRelationId: Oid = 3079; // pg_extension
pub const ForeignTableRelationId: Oid = 3118; // pg_foreign_table
pub const PolicyRelationId: Oid = 3256; // pg_policy
pub const PartitionedRelationId: Oid = 3350; // pg_partitioned_table
pub const StatisticExtRelationId: Oid = 3381; // pg_statistic_ext
pub const InitPrivsRelationId: Oid = 3394; // pg_init_privs
pub const StatisticExtDataRelationId: Oid = 3429; // pg_statistic_ext_data
pub const CollationRelationId: Oid = 3456; // pg_collation
pub const EventTriggerRelationId: Oid = 3466; // pg_event_trigger
pub const EnumRelationId: Oid = 3501; // pg_enum
pub const RangeRelationId: Oid = 3541; // pg_range
pub const TransformRelationId: Oid = 3576; // pg_transform
pub const SharedSecLabelRelationId: Oid = 3592; // pg_shseclabel
pub const SecLabelRelationId: Oid = 3596; // pg_seclabel
pub const TSDictionaryRelationId: Oid = 3600; // pg_ts_dict
pub const TSParserRelationId: Oid = 3601; // pg_ts_parser
pub const TSConfigRelationId: Oid = 3602; // pg_ts_config
pub const TSConfigMapRelationId: Oid = 3603; // pg_ts_config_map
pub const TSTemplateRelationId: Oid = 3764; // pg_ts_template
pub const ReplicationOriginRelationId: Oid = 6000; // pg_replication_origin
pub const SubscriptionRelationId: Oid = 6100; // pg_subscription
pub const SubscriptionRelRelationId: Oid = 6102; // pg_subscription_rel
pub const PublicationRelationId: Oid = 6104; // pg_publication
pub const PublicationRelRelationId: Oid = 6106; // pg_publication_rel
pub const PublicationNamespaceRelationId: Oid = 6237; // pg_publication_namespace
pub const ParameterAclRelationId: Oid = 6243; // pg_parameter_acl
