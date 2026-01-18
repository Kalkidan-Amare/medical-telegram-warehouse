import csv
import os
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv
from ultralytics import YOLO


PRODUCT_CLASSES = {"bottle", "cup", "bowl", "box", "container"}


def load_model(model_path: str) -> YOLO:
    return YOLO(model_path)


def categorize_image(detected_classes: List[str]) -> str:
    has_person = "person" in detected_classes
    has_product = any(cls in PRODUCT_CLASSES for cls in detected_classes)

    if has_person and has_product:
        return "promotional"
    if has_product and not has_person:
        return "product_display"
    if has_person and not has_product:
        return "lifestyle"
    return "other"


def scan_images(images_root: Path) -> List[Path]:
    return [path for path in images_root.rglob("*.jpg")]


def run_detection(model: YOLO, image_path: Path) -> Dict[str, object]:
    results = model.predict(str(image_path), verbose=False)
    detections = results[0]

    classes = [
        detections.names[int(cls)]
        for cls in detections.boxes.cls.tolist()
    ]
    confidences = detections.boxes.conf.tolist() if detections.boxes else []

    max_conf = max(confidences) if confidences else 0.0
    category = categorize_image(classes)

    return {
        "image_path": str(image_path),
        "detected_classes": ",".join(classes),
        "confidence_score": max_conf,
        "image_category": category,
    }


def extract_message_id(image_path: Path) -> int:
    return int(image_path.stem)


def main() -> None:
    load_dotenv()
    images_root = Path(os.getenv("IMAGES_PATH", "data/raw/images"))
    output_csv = Path(os.getenv("YOLO_OUTPUT_CSV", "data/processed/yolo_detections.csv"))
    model_path = os.getenv("YOLO_MODEL", "yolov8n.pt")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    model = load_model(model_path)

    with output_csv.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "message_id",
                "image_path",
                "detected_classes",
                "confidence_score",
                "image_category",
            ],
        )
        writer.writeheader()

        for image_path in scan_images(images_root):
            result = run_detection(model, image_path)
            writer.writerow(
                {
                    "message_id": extract_message_id(image_path),
                    **result,
                }
            )

    print(f"Saved detections to {output_csv}")


if __name__ == "__main__":
    main()
