import os
from PIL import Image, ImageFilter
from typing import Optional

def apply_blur(input_path: str, output_path: str, radius: int = 10) -> Optional[str]:
    try:
        with Image.open(input_path) as img:
            blurred_img = img.filter(ImageFilter.GaussianBlur(radius))
            output_dir = os.path.dirname(output_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            blurred_img.save(output_path)
        
        return output_path

    except FileNotFoundError:
        return None
    except Exception:
        return None