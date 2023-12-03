#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    logger.info("starting wandb session...")
    run = wandb.init(project="nyc_airbnb", group="data_cleaning", job_type="basic_cleaning", save_code=True)
    logger.info("...wandb session started.")
    
    logger.info("updating config session parameters ...")
    run.config.update(args)
    logger.info("...config session parameters up to date.")

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("downloading artifact...")
    artifact_local_path = run.use_artifact(args.input_artifact).file(root="./artifacts/sample.csv")
    df = pd.read_csv(artifact_local_path, index_col="id")

    # Drop outliers
    logger.info("dropping outliers from artifact...")   
    min_price = args.min_price
    max_price = args.max_price
    idx = df["price"].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    logger.info("converting last_review to datetime...")   
    df["last_review"] = pd.to_datetime(df["last_review"])
    
    # drop rows where coordinates are out of boundaries
    logger.info("dropping records with out of boundaries coordinates...")   
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()
    
    df.to_csv("clean_sample.csv", index="id")

    # upload output to W&B
    logger.info("upload output to W&B...")   
    output_artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    output_artifact.add_file("clean_sample.csv")
    run.log_artifact(output_artifact)
    
    run.finish()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="input data artifact, before cleaning",
        required=True,
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="output data artifact, after cleaning",
        required=True,
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="type of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="decription of the output artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="minimum rent price to filter out input data from outliers",
        required=True,
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="maximum rent price to filter out input data from outliers",
        required=True,
    )

    args = parser.parse_args()

    go(args)
