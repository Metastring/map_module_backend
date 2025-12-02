"""
Classification service for computing class breaks.
Implements Equal Interval, Quantile, and Jenks Natural Breaks algorithms.
"""
from typing import List, Optional, Tuple
import logging
from ..models.schema import ClassificationMethod, ClassificationResult
from .color_palettes import get_colors

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service for computing classification breaks for map styling.
    Mimics GeoServer's classification capabilities.
    """

    def classify(
        self,
        method: ClassificationMethod,
        num_classes: int,
        values: Optional[List[float]] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        categories: Optional[List[str]] = None,
        quantile_breaks: Optional[List[float]] = None,
        palette_name: str = "YlOrRd",
        custom_colors: Optional[List[str]] = None,
        manual_breaks: Optional[List[float]] = None,
    ) -> ClassificationResult:
        """
        Compute classification breaks based on the specified method.
        
        Args:
            method: Classification method to use
            num_classes: Number of classes
            values: All values (needed for Jenks)
            min_value: Minimum value (for Equal Interval)
            max_value: Maximum value (for Equal Interval)
            categories: Distinct category values (for Categorical)
            quantile_breaks: Pre-computed quantile breaks from DB
            palette_name: ColorBrewer palette name
            custom_colors: Custom color list (overrides palette)
            manual_breaks: Manual class breaks (for Manual method)
        
        Returns:
            ClassificationResult with breaks/categories and colors
        """
        # Get colors
        if custom_colors and len(custom_colors) >= num_classes:
            colors = custom_colors[:num_classes]
        else:
            colors = get_colors(palette_name, num_classes)

        if method == ClassificationMethod.CATEGORICAL:
            return self._classify_categorical(categories or [], colors)
        
        elif method == ClassificationMethod.MANUAL:
            return self._classify_manual(manual_breaks or [], colors, min_value, max_value)
        
        elif method == ClassificationMethod.EQUAL_INTERVAL:
            return self._classify_equal_interval(
                num_classes, min_value or 0, max_value or 100, colors
            )
        
        elif method == ClassificationMethod.QUANTILE:
            if quantile_breaks:
                return self._classify_from_breaks(
                    quantile_breaks, colors, min_value, max_value, method
                )
            elif values:
                return self._classify_quantile(num_classes, values, colors)
            else:
                # Fallback to equal interval
                return self._classify_equal_interval(
                    num_classes, min_value or 0, max_value or 100, colors
                )
        
        elif method == ClassificationMethod.JENKS:
            if values:
                return self._classify_jenks(num_classes, values, colors)
            else:
                # Fallback to equal interval
                return self._classify_equal_interval(
                    num_classes, min_value or 0, max_value or 100, colors
                )
        
        # Default fallback
        return self._classify_equal_interval(
            num_classes, min_value or 0, max_value or 100, colors
        )

    def _classify_categorical(
        self, 
        categories: List[str], 
        colors: List[str]
    ) -> ClassificationResult:
        """Classify categorical data."""
        # Ensure we have enough colors
        while len(colors) < len(categories):
            colors = colors + colors  # Repeat colors if needed
        
        return ClassificationResult(
            method=ClassificationMethod.CATEGORICAL,
            categories=categories,
            colors=colors[:len(categories)],
            num_classes=len(categories)
        )

    def _classify_manual(
        self,
        breaks: List[float],
        colors: List[str],
        min_value: Optional[float],
        max_value: Optional[float]
    ) -> ClassificationResult:
        """Use manual class breaks."""
        sorted_breaks = sorted(breaks)
        num_classes = len(sorted_breaks) + 1
        
        # Adjust colors
        while len(colors) < num_classes:
            colors = colors + colors
        
        return ClassificationResult(
            method=ClassificationMethod.MANUAL,
            breaks=sorted_breaks,
            colors=colors[:num_classes],
            min_value=min_value,
            max_value=max_value,
            num_classes=num_classes
        )

    def _classify_equal_interval(
        self,
        num_classes: int,
        min_value: float,
        max_value: float,
        colors: List[str]
    ) -> ClassificationResult:
        """
        Compute equal interval classification.
        Divides the range into equal-sized intervals.
        """
        if min_value == max_value:
            return ClassificationResult(
                method=ClassificationMethod.EQUAL_INTERVAL,
                breaks=[min_value],
                colors=colors[:1],
                min_value=min_value,
                max_value=max_value,
                num_classes=1
            )
        
        interval = (max_value - min_value) / num_classes
        breaks = [min_value + interval * i for i in range(1, num_classes)]
        
        return ClassificationResult(
            method=ClassificationMethod.EQUAL_INTERVAL,
            breaks=breaks,
            colors=colors[:num_classes],
            min_value=min_value,
            max_value=max_value,
            num_classes=num_classes
        )

    def _classify_quantile(
        self,
        num_classes: int,
        values: List[float],
        colors: List[str]
    ) -> ClassificationResult:
        """
        Compute quantile classification.
        Each class contains approximately the same number of features.
        """
        sorted_values = sorted([v for v in values if v is not None])
        
        if len(sorted_values) == 0:
            return ClassificationResult(
                method=ClassificationMethod.QUANTILE,
                breaks=[],
                colors=colors[:1],
                num_classes=1
            )
        
        min_value = sorted_values[0]
        max_value = sorted_values[-1]
        
        if min_value == max_value:
            return ClassificationResult(
                method=ClassificationMethod.QUANTILE,
                breaks=[min_value],
                colors=colors[:1],
                min_value=min_value,
                max_value=max_value,
                num_classes=1
            )
        
        # Compute quantile breaks
        breaks = []
        n = len(sorted_values)
        for i in range(1, num_classes):
            idx = int(i * n / num_classes)
            if idx < n:
                breaks.append(sorted_values[idx])
        
        # Remove duplicates while preserving order
        breaks = list(dict.fromkeys(breaks))
        
        return ClassificationResult(
            method=ClassificationMethod.QUANTILE,
            breaks=breaks,
            colors=colors[:len(breaks) + 1],
            min_value=min_value,
            max_value=max_value,
            num_classes=len(breaks) + 1
        )

    def _classify_jenks(
        self,
        num_classes: int,
        values: List[float],
        colors: List[str]
    ) -> ClassificationResult:
        """
        Compute Jenks Natural Breaks classification.
        Minimizes within-class variance while maximizing between-class variance.
        """
        sorted_values = sorted([v for v in values if v is not None])
        
        if len(sorted_values) == 0:
            return ClassificationResult(
                method=ClassificationMethod.JENKS,
                breaks=[],
                colors=colors[:1],
                num_classes=1
            )
        
        min_value = sorted_values[0]
        max_value = sorted_values[-1]
        
        if min_value == max_value or len(sorted_values) <= num_classes:
            return ClassificationResult(
                method=ClassificationMethod.JENKS,
                breaks=[min_value],
                colors=colors[:1],
                min_value=min_value,
                max_value=max_value,
                num_classes=1
            )
        
        try:
            breaks = self._jenks_breaks(sorted_values, num_classes)
        except Exception as e:
            logger.warning(f"Jenks calculation failed, falling back to quantile: {e}")
            return self._classify_quantile(num_classes, sorted_values, colors)
        
        return ClassificationResult(
            method=ClassificationMethod.JENKS,
            breaks=breaks,
            colors=colors[:len(breaks) + 1],
            min_value=min_value,
            max_value=max_value,
            num_classes=len(breaks) + 1
        )

    def _jenks_breaks(self, values: List[float], num_classes: int) -> List[float]:
        """
        Calculate Jenks natural breaks using the classic algorithm.
        Based on the Fisher-Jenks algorithm.
        """
        n = len(values)
        
        if n <= num_classes:
            return values[1:]
        
        # Initialize matrices
        lower_class_limits = [[0.0] * (num_classes + 1) for _ in range(n + 1)]
        variance_combinations = [[float('inf')] * (num_classes + 1) for _ in range(n + 1)]
        
        for i in range(1, num_classes + 1):
            lower_class_limits[1][i] = 1
            variance_combinations[1][i] = 0
        
        for i in range(2, n + 1):
            for j in range(1, num_classes + 1):
                variance_combinations[i][j] = float('inf')
        
        # Calculate variance combinations
        for l in range(2, n + 1):
            sum_val = 0.0
            sum_sq = 0.0
            
            for m in range(1, l + 1):
                i = l - m + 1
                val = values[i - 1]
                sum_val += val
                sum_sq += val * val
                variance = sum_sq - (sum_val * sum_val) / m
                
                if i > 1:
                    for j in range(2, num_classes + 1):
                        if variance_combinations[l][j] >= variance + variance_combinations[i - 1][j - 1]:
                            lower_class_limits[l][j] = i
                            variance_combinations[l][j] = variance + variance_combinations[i - 1][j - 1]
            
            lower_class_limits[l][1] = 1
            variance_combinations[l][1] = sum_sq - (sum_val * sum_val) / l
        
        # Extract breaks
        k = n
        breaks = []
        
        for j in range(num_classes, 1, -1):
            idx = int(lower_class_limits[k][j]) - 1
            if 0 <= idx < n - 1:
                break_val = values[idx]
                if not breaks or break_val != breaks[-1]:
                    breaks.append(break_val)
            k = int(lower_class_limits[k][j]) - 1
        
        breaks.reverse()
        return breaks

    def _classify_from_breaks(
        self,
        breaks: List[float],
        colors: List[str],
        min_value: Optional[float],
        max_value: Optional[float],
        method: ClassificationMethod
    ) -> ClassificationResult:
        """Create classification result from pre-computed breaks."""
        num_classes = len(breaks) + 1
        
        return ClassificationResult(
            method=method,
            breaks=breaks,
            colors=colors[:num_classes],
            min_value=min_value,
            max_value=max_value,
            num_classes=num_classes
        )
