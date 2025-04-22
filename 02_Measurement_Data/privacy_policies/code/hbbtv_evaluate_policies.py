import re
import string
import sys
import json
import numpy as np
import pandas as pd
import torch
import transformers
from tqdm import tqdm

from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoConfig, AutoModel

from tinydb import TinyDB, Query
from tinydb import where as tinydb_where
from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

from textacy import preprocessing as textacy_preprocessing
import ftfy

from models import BERTForSequenceClassification_feature

# set logging level to only show errors
transformers.logging.set_verbosity_error()

measurement = sys.argv[1]
db_path = "./data/Measurement_" + measurement + "/hbbtv_policies_database_" + measurement + ".json"


MODELS = {
    "MODELS_CATEGORIES_AND_ATTRIBUTES":{
        "de": {
            'First_party_collection_or_use': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_First Party.pth',
            'Third_party_collection_or_use': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Third Party.pth',
            'Information_type': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type.pth',
            'Purpose': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose.pth',
            'Collection_process': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Collection Process.pth',
            'Legal_basis_for_collection': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Legal Basis for Collection.pth',
            'Third_party_entity': 'MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Third-Party Entity.pth',
        },
        "en": {
            'First_party_collection_or_use': 'MAP_English_bert-base-uncased_5_False_False_1e-05_First Party.pth',
            'Third_party_collection_or_use': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Third Party.pth',
            'Information_type': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Information Type.pth',
            'Purpose': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Purpose.pth',
            'Collection_process': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Collection Process.pth',
            'Legal_basis_for_collection': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Legal Basis for Collection.pth',
            'Third_party_entity': 'MAP_English_bert-base-uncased_5_False_False_1e-05_Third-Party Entity.pth',
        }
    },
    "MODELS_WITH_VALUE_NAMES":{
        "de": {
            "Purpose_Essential_service_or_feature": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Essential service or feature_diff.pth",
            "Purpose_Advertising_or_marketing":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Advertising or marketing_diff.pth",
            "Purpose_Analytics_or_research":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Analytics or research_diff.pth",
            "Purpose_Service_operation_and_security":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Service operation and security_diff.pth",
            "Purpose_Legal_requirement" :"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Legal requirement_diff.pth",
            "Information_type_Financial":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Financial_diff.pth",
            "Collection_Process_Shared_by_first_party_with_a_third_party":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Collection Process_Shared by first party with a third party_diff.pth",
            "Information_type_Contact_information": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Contact information_diff.pth",
            "Information_type_Location": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Location_diff.pth",
            "Information_type_Demographic_data": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Demographic data_diff.pth",
            "Information_type_User_online_activities": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_User online activities_diff.pth",
            "Information_type_IP_address_and_device_IDs":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_IP address and device IDs_diff.pth",
            "Information_type_Cookies_and_tracking_elements": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Cookies and tracking elements_diff.pth",
            "Information_type_Computer_information": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Computer information_diff.pth",
            "Information_type_Generic_personal_information": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Generic personal information_diff.pth",
            "Information_type_Computer_information": "MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Computer information_diff.pth",
            "Collection_Process_Collected_on_first_party_website_app":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Collection Process_Collected on first-party website_app_diff.pth",
            "Legal_basis_for_collection_Legitimate_interests_of_first_or_third_party":"MAP_German_bert-base-multilingual-uncased_5_False_False_5e-06_Legal Basis for Collection_Legitimate interests of first or third party_diff.pth"
        },
        "en": {
            "Purpose_Advertising_or_marketing": "MAP_English_bert-base-uncased_5_False_False_5e-06_Purpose_Advertising or marketing.pth",
            "Purpose_Analytics_or_research": "MAP_English_bert-base-uncased_5_False_False_5e-06_Purpose_Analytics or research.pth",
            "Purpose_Service_operation_and_security": "MAP_English_bert-base-uncased_5_False_False_5e-06_Purpose_Service operation and security.pth",
            "Information_type_Contact_information": "MAP_English_bert-base-uncased_5_False_False_5e-06_Information Type_Contact information.pth",
            "Information_type_Location": "MAP_English_bert-base-uncased_5_False_False_5e-06_Information Type_Location.pth",
            "Information_type_Demographic_data": "MAP_English_bert-base-uncased_5_False_False_5e-06_Information Type_Demographic data.pth",
            "Information_type_Generic_personal_information": "MAP_English_bert-base-uncased_5_False_False_5e-06_Information Type_Generic personal information.pth",
        }
    },
    "MODELS_WITH_FEATURE_VALUES":{
        "de":{
            #"Purpose_Advertising_or_marketing": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Advertising or marketing_diff.pth",
            # "Purpose_Analytics_or_research": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Analytics or research_diff.pth",
            # "Purpose_Service_operation_and_security": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Service operation and security_diff.pth",
            # "Purpose_Legal_requirement": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Purpose_Legal requirement_diff.pth",
            # "Collection_Process_Collected_on_first_party_website_app": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Collection Process_Collected on first-party website_app_diff.pth",
            # "Collection_Process_Shared_by_first_party_with_a_third_party": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Collection Process_Shared by first party with a third party_diff.pth",
            # "Information_type_Financial":"MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Information Type_Financial_diff.pth",
            # "Legal_basis_for_collection_Legitimate_interests_of_first_or_third_party": "MAP_German_value_feature_bert-base-multilingual-uncased_5_False_False_5e-06_Legal Basis for Collection_Legitimate interests of first or third party_diff.pth",

        },
        "en": {
            "Purpose_Essential_service_or_feature": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Purpose_Essential service or feature.pth",
            "Purpose_Legal_requirement":"MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Purpose_Legal requirement.pth",
            "Collection_Process_Collected_on_first_party_website_app": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Collection Process_Collected on first-party website_app.pth",
            "Collection_Process_Shared_by_first_party_with_a_third_party": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Collection Process_Shared by first party with a third party.pth",
            "Information_type_Financial": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Information Type_Financial.pth",
            "Information_type_User_online_activities": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Information Type_User online activities.pth",
            "Information_type_IP_address_and_device_IDs": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Information Type_IP address and device IDs.pth",
            "Information_type_Cookies_and_tracking_elements": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Information Type_Cookies and tracking elements.pth",
            "Legal_basis_for_collection_Legitimate_interests_of_first_or_third_party": "MAP_English_value_feature_bert-base-uncased_5_False_False_5e-06_Legal Basis for Collection_Legitimate interests of first or third party.pth",
        }
    }
}

def text_cleaner(text):
    """
    Customized code to preprocess the extracted texts lightly: 
    Note that the preprocessing does not lead to changing the semtantic of the texts as this would lead to the extraction of incorrect sentence embeddings using BERT.
    """
    text = textacy_preprocessing.normalize.bullet_points(text)
    text = textacy_preprocessing.normalize.unicode(text)
    text = ftfy.fix_text(text) # fix unicode errors and other special characters
    text = text.replace("\n", "\n\n") # for splitting by linebreak
    text = textacy_preprocessing.normalize.hyphenated_words(text) # other hyphenated words
    text = textacy_preprocessing.normalize.whitespace(text)
    text = re.sub(" +", " ", "".join(x if x.isprintable() or x in string.whitespace else " " for x in text))
    return text


def text_splitter(policy_dict):
    policy_dict["text"] = text_cleaner(policy_dict["text"])
    list_of_passages = policy_dict["text"].splitlines()
    list_of_passages = [passage.replace("\n", " ") for passage in list_of_passages]
    list_of_passages = list(filter(None, list_of_passages))
    policy_dict["passages"] = list_of_passages
    return policy_dict


def tokenize_policies(policy_dict, tokenizer):
    try:
        policy_dict["tokenized_passages"] = tokenizer(policy_dict["passages"], truncation=True, padding=True)
    except IndexError:
        print(f"{policy_dict['text_id']} is empty. Skipping tokenization", flush=True)
        policy_dict["tokenized_passages"] = [[]]

    return policy_dict


def load_policies(language):
    """
    Load policies in the passed language and their metadata
    """

    def load_metadata_with_FN_labels_corrected():
        df_metadata = pd.read_csv("./data/Measurement_" + measurement + "/hbbtv_" + measurement + "_metadata_FN_labels_corrected.tsv", sep="\t")
        return df_metadata

    df_metadata = load_metadata_with_FN_labels_corrected()
    df_metadata_corrected = df_metadata.loc[(df_metadata["predicted_label"] != df_metadata["corrected_predicted_label"]) & (df_metadata["determined_language"]== language)]

    print(f"df_metadata_corrected: {df_metadata_corrected.shape}", flush=True)

    list_of_text_ids = df_metadata_corrected["text_id"].tolist()

    db = TinyDB(db_path, storage=CachingMiddleware(JSONStorage))
    policies_table = db.table("policies")

    list_of_policy_dicts = policies_table.search(Query().text_id.one_of(list_of_text_ids))
    print(f"Loaded {len(list_of_policy_dicts)} policies for language {language}", flush=True)
    db.close()

    return list_of_policy_dicts


def generate_pred_from_classifier(tokenized_passage, language, model_name, device, batch_size=4):
    """ Generate the data practices evaluations """

    input_id_features = tokenized_passage['input_ids']
    segment_ids_feature = tokenized_passage['token_type_ids']
    input_mask_features = tokenized_passage['attention_mask']

    # print(model, flush=True)
    # print(list(model.named_parameters()))

    evaluations_complete = []
    language_model_dict = MODELS["MODELS_CATEGORIES_AND_ATTRIBUTES"][language]
    config = AutoConfig.from_pretrained(model_name, num_labels=2)
    model = AutoModelForSequenceClassification.from_pretrained(model_name, from_tf=bool(".ckpt" in model_name), config=config).to(device)
    for category, model_file in language_model_dict.items():
        print(f"Evaluating: {category} and loading code/resources/models/{model_file}", flush=True)
        model.load_state_dict(torch.load(f'code/resources/models/{model_file}', map_location=device))
        model = model.eval()
        evaluations_array = []
        for batch_no in range(int((len(input_id_features) + batch_size - 1) / batch_size)):
            # print(f"Batch number {batch_no}", flush=True)
            max_id = min(((batch_no + 1) * batch_size), len(input_id_features))
            # print(f"Max id: {max_id}", flush=True)
            range_id = max_id - (batch_no * batch_size)
            
            # print(torch.LongTensor(input_id_features[(batch_no * batch_size) : max_id]).to(device))
            # print(torch.LongTensor(input_mask_features[(batch_no * batch_size) : max_id]).to(device))
            # print(torch.LongTensor(segment_ids_feature[(batch_no * batch_size) : max_id]).to(device))
            outputs = model(input_ids=torch.LongTensor(input_id_features[(batch_no * batch_size) : max_id]).to(device),
                        token_type_ids=torch.LongTensor(segment_ids_feature[(batch_no * batch_size) : max_id]).to(device),
                        attention_mask=torch.LongTensor(input_mask_features[(batch_no * batch_size) : max_id]).to(device))
            evaluations = (outputs["logits"][:, 1] > outputs["logits"][:, 0]).int().cpu().numpy()
            evaluations_array.append(evaluations)
        evaluations = np.concatenate(evaluations_array)
        print(evaluations.shape)
        evaluations_complete.append(evaluations.reshape(-1, 1))
    data_type_feature = np.concatenate(evaluations_complete, axis=-1)
    print(f"Shape of data_type_feature_categories_and_attributes: {data_type_feature.shape}", flush=True)
    evaluations = data_type_feature.tolist()
    evaluation_df = pd.DataFrame(evaluations, columns=language_model_dict.keys())
    evaluation_df_categories_and_attributes = evaluation_df.agg(['sum']).reset_index(drop=True)


    evaluations_complete = []
    language_model_dict = MODELS["MODELS_WITH_VALUE_NAMES"][language]
    config = AutoConfig.from_pretrained(model_name, num_labels=2)
    model = AutoModelForSequenceClassification.from_pretrained(model_name, from_tf=bool(".ckpt" in model_name), config=config).to(device)
    for category, model_file in language_model_dict.items():
        print(f"Evaluating: {category} and loading code/resources/models/{model_file}", flush=True)
        model.load_state_dict(torch.load(f'code/resources/models/{model_file}', map_location=device))
        model = model.eval()
        evaluations_array = []
        for batch_no in range(int((len(input_id_features) + batch_size - 1) / batch_size)):
            # print(f"Batch number {batch_no}", flush=True)
            max_id = min(((batch_no + 1) * batch_size), len(input_id_features))
            # print(f"Max id: {max_id}", flush=True)
            range_id = max_id - (batch_no * batch_size)
            
            # print(torch.LongTensor(input_id_features[(batch_no * batch_size) : max_id]).to(device))
            # print(torch.LongTensor(input_mask_features[(batch_no * batch_size) : max_id]).to(device))
            # print(torch.LongTensor(segment_ids_feature[(batch_no * batch_size) : max_id]).to(device))
            outputs = model(input_ids=torch.LongTensor(input_id_features[(batch_no * batch_size) : max_id]).to(device),
                        token_type_ids=torch.LongTensor(segment_ids_feature[(batch_no * batch_size) : max_id]).to(device),
                        attention_mask=torch.LongTensor(input_mask_features[(batch_no * batch_size) : max_id]).to(device))
            evaluations = (outputs["logits"][:, 1] > outputs["logits"][:, 0]).int().cpu().numpy()
            evaluations_array.append(evaluations)
        evaluations = np.concatenate(evaluations_array)
        print(evaluations.shape)
        evaluations_complete.append(evaluations.reshape(-1, 1))
    data_type_feature = np.concatenate(evaluations_complete, axis=-1)
    print(f"Shape of data_type_value_names: {data_type_feature.shape}", flush=True)
    evaluations = data_type_feature.tolist()
    evaluation_df = pd.DataFrame(evaluations, columns=language_model_dict.keys())
    evaluation_df_value_names = evaluation_df.agg(['sum']).reset_index(drop=True)

    if language == "en":
        evaluations_complete = []
        language_model_dict = MODELS["MODELS_WITH_FEATURE_VALUES"][language]
        for category, model_file in language_model_dict.items():
            print(f"Evaluating: {category} and loading code/resources/models/{model_file}", flush=True)
            config = AutoConfig.from_pretrained(model_name, num_labels=2)
            bert_model = AutoModel.from_pretrained(model_name, from_tf=bool(".ckpt" in model_name), config=config).to(device)
            model = BERTForSequenceClassification_feature(bert_model, data_type_feature.shape[1]).to(device)
            # model.load_state_dict(torch.load(f'code/resources/models/{model_file}', map_location=device))
            model.load_state_dict(torch.load(f'code/resources/models/{model_file}'))
            model = model.eval()
            evaluations_array = []
            for batch_no in range(int((len(input_id_features) + batch_size - 1) / batch_size)):
                # print(f"Batch number {batch_no}", flush=True)
                max_id = min(((batch_no + 1) * batch_size), len(input_id_features))
                # print(f"Max id: {max_id}", flush=True)
                range_id = max_id - (batch_no * batch_size)

                outputs = model(input_ids=torch.LongTensor(input_id_features[(batch_no * batch_size) : max_id]).to(device),
                            token_type_ids=torch.LongTensor(segment_ids_feature[(batch_no * batch_size) : max_id]).to(device),
                            attention_mask=torch.LongTensor(input_mask_features[(batch_no * batch_size) : max_id]).to(device),
                            features=torch.LongTensor(data_type_feature[(batch_no * batch_size) : max_id]).to(device),
                            )
                evaluations = (outputs["logits"][:, 1] > outputs["logits"][:, 0]).int().cpu().numpy()
                evaluations_array.append(evaluations)
            evaluations = np.concatenate(evaluations_array)
            print(evaluations.shape)
            evaluations_complete.append(evaluations.reshape(-1, 1))
        data_type_feature = np.concatenate(evaluations_complete, axis=-1)
        print(f"Shape of data_type_feature_values: {data_type_feature.shape}", flush=True)
        evaluations = data_type_feature.tolist()
        evaluation_df = pd.DataFrame(evaluations, columns=language_model_dict.keys())
        evaluation_df_feature_values = evaluation_df.agg(['sum']).reset_index(drop=True)
    
        evaluation_df = pd.concat([evaluation_df_categories_and_attributes, evaluation_df_value_names, evaluation_df_feature_values], axis=1)

    else:
        evaluation_df = pd.concat([evaluation_df_categories_and_attributes, evaluation_df_value_names], axis=1)

    # print(evaluation_df_values.to_string(), flush=True)

    return evaluation_df


def save_evaluations(list_of_policy_evaluations_dicts):
    print("Saving evaluations in database ...", flush=True)
    storage = CachingMiddleware(JSONStorage)
    storage.WRITE_CACHE_SIZE = 25
    db = TinyDB(db_path, storage=storage)
    evaluation_table = db.table("policies_evaluations")

    for policy_evaluation_dict in list_of_policy_evaluations_dicts:
        evaluation_table.upsert(policy_evaluation_dict, tinydb_where("text_id") == policy_evaluation_dict["text_id"])

    db.close()


def save_results(list_of_policy_dicts, list_of_policy_evaluations_dicts, language, measurement):
    print("Saving evalutions as CSV and JSON ...", flush=True)
    df_policies = pd.DataFrame(list_of_policy_dicts)
    df_policy_evaluations = pd.DataFrame(list_of_policy_evaluations_dicts)

    df = pd.merge(df_policies, df_policy_evaluations, how="inner", on=["text_id"], validate="one_to_one")
    df.drop(["passages", "text_canola", "text_readability", "text_markdown"], axis=1, inplace=True)
    df.to_json("results/policies_evaluations_" + language + "_" + measurement + "_FN_labels_corrected.json", orient="records", lines=True, force_ascii=False)
    df.drop(["text", "tokenized_passages"], axis=1, inplace=True)
    df.to_csv("results/policies_evaluations_" + language + "_" + measurement + "_FN_labels_corrected.csv", sep=";", index=False, encoding="utf-8")


def main():
    print(f"Is CUDA available? {torch.cuda.is_available()}", flush=True)
    device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    # device = torch.device("cpu")

    # language = "de"
    language = "en"

    list_of_policy_evaluation_dicts = []
    print(f"Working on {language} policies...", flush=True)
    # set model name based on language
    if language == "de":
        model_name = 'bert-base-multilingual-uncased'
    elif language == "en":
        model_name = 'bert-base-uncased'

    tokenizer = AutoTokenizer.from_pretrained(model_name, use_fast=True)

    # load policies
    list_of_policy_dicts = load_policies(language)

    # preprocess and split texts into passages
    list_of_policy_dicts = [text_splitter(policy_dict) for policy_dict in tqdm(list_of_policy_dicts, desc="Preprocessing and splitting passages...")]

    # tokenize the passages into tokens
    list_of_policy_dicts = [tokenize_policies(policy_dict, tokenizer) for policy_dict in tqdm(list_of_policy_dicts, desc="Tokenizing passages...")]

    for policy_dict in tqdm(list_of_policy_dicts, desc="Generating evaluations..."):
        print(policy_dict["channel"], policy_dict["sha1"], flush=True)
        evaluation_df = generate_pred_from_classifier(policy_dict["tokenized_passages"], language, model_name, device)
        evaluation_df["text_id"] = policy_dict["text_id"]
        evaluation_dict = evaluation_df.to_dict("records")[0]
        print(evaluation_dict, flush=True)
        list_of_policy_evaluation_dicts.append(evaluation_dict)

    save_results(list_of_policy_dicts, list_of_policy_evaluation_dicts, language, measurement)

    # save_evaluations(list_of_policy_evaluation_dicts)


if __name__ == "__main__":
    main()
