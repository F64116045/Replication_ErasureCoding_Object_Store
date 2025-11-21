# Helper script to inspect all 6 storage nodes and list residual files in /data/ directories.
# Useful for debugging why "Storage usage check (after deletion)" failed (i.e., usage != 0).

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${CYAN}===========================================${NC}"
echo -e "${CYAN} Checking residual files on 6 storage nodes...${NC}"
echo -e "${CYAN}===========================================${NC}"

# Loop through nodes 1 to 6
for i in {1..6}
do
  SERVICE_NAME="storage_node_${i}"
  DATA_DIR="/data/node${i}"
  
  echo -e "\n${GREEN}--- Checking Node: ${SERVICE_NAME} (Dir: ${DATA_DIR}) ---${NC}"
  
  # Execute 'ls -l' inside the container to show file details (permissions, owner, size, name)
  # We use 'docker-compose exec' to run the command inside the running container.
  docker-compose exec $SERVICE_NAME ls -l $DATA_DIR
done

echo -e "\n${RED}--- Check Complete ---${NC}"
echo -e "${RED}Sum the file sizes in the output above to identify the residual data.${NC}"
echo -e "${RED}Note: If you see '_cold_chunk_' files, it indicates a bug in the API deletion logic.${NC}"