import {memo} from 'react'
import {Box, Text} from 'ink'
import {NetworkInfo} from '../lib/docker.js'

interface Props {
  name: string
  info: NetworkInfo
}

export const NetworkPanel = memo(function NetworkPanel({name, info}: Props) {
  return (
    <Box gap={2} marginTop={1}>
      <Text color="gray">network</Text>
      <Text color="cyan">{name}</Text>
      {info.exists ? (
        <>
          <Text color="green">●</Text>
          {info.subnet && <Text color="gray">{info.subnet}</Text>}
          {info.gateway && (
            <>
              <Text color="gray">gateway</Text>
              <Text color="yellow">{info.gateway}</Text>
              <Text color="gray">(host machine IP for containers)</Text>
            </>
          )}
        </>
      ) : (
        <>
          <Text color="red">○</Text>
          <Text color="gray">not created — run scaffold up</Text>
        </>
      )}
    </Box>
  )
})
