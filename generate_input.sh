#!/bin/bash

OUTPUT_FILE="input.txt"
TARGET_SIZE_MB=100
TARGET_SIZE_BYTES=$((TARGET_SIZE_MB * 1024 * 1024))

# Define your 200-word vocabulary
WORDS=(alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega apple orange banana mango lemon kiwi grape berry melon pear plum apricot avocado cherry fig date coconut lime peach pineapple strawberry blueberry raspberry watermelon corn rice wheat barley oat rye millet sorghum pasta bread noodle egg cheese milk butter yogurt tofu chicken beef pork lamb fish crab shrimp oyster mussel scallop salt sugar pepper spice chili curry basil thyme rosemary parsley mint garlic onion tomato potato carrot spinach lettuce cabbage broccoli kale celery cucumber radish beet pea bean lentil chickpea mushroom cornmeal flour oil vinegar sauce soup stew roast fry bake grill steam boil rose daisy tulip lily orchid jasmine sunflower marigold poppy cactus bamboo pine cedar oak maple birch ash elm cedar cotton silk linen denim velvet wool nylon leather plastic paper glass steel copper bronze iron silver gold quartz ruby emerald topaz sapphire opal ink pen pencil marker chalk crayon board book bag chair table desk shelf window door frame phone cable plug charger light lamp bulb screen camera mirror clock ball bat glove racket shoes socks pants shorts jeans hoodie hat coat ring watch soap brush towel mirror comb clip)

WORD_COUNT=${#WORDS[@]}

echo "Generating $TARGET_SIZE_MB MB file..."
> "$OUTPUT_FILE"

get_file_size() {
  stat -f%z "$OUTPUT_FILE"
}

# Loop until file size >= target size
while true; do
  CURRENT_SIZE=$(get_file_size)
  [ -z "$CURRENT_SIZE" ] && CURRENT_SIZE=0

  if [ "$CURRENT_SIZE" -ge "$TARGET_SIZE_BYTES" ]; then
    break
  fi

  LINE=""
  for i in {1..100}; do
    JOIN_TYPE=$((RANDOM % 4))
    if [ $JOIN_TYPE -eq 0 ]; then
      w1=${WORDS[$RANDOM % WORD_COUNT]}
      w2=${WORDS[$RANDOM % WORD_COUNT]}
      if [ $((RANDOM % 2)) -eq 0 ]; then
        w3=${WORDS[$RANDOM % WORD_COUNT]}
        WORD="${w1}${w2}${w3}"
      else
        WORD="${w1}${w2}"
      fi
    else
      WORD=${WORDS[$RANDOM % WORD_COUNT]}
    fi
    LINE+="$WORD "
  done

  echo "$LINE" >> "$OUTPUT_FILE"
done

echo "Done. File '$OUTPUT_FILE' is now $(du -h "$OUTPUT_FILE" | cut -f1)"
